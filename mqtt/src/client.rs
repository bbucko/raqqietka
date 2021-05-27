use core::MQTTError;
use std::convert::TryFrom;
use std::fmt;
use std::fmt::Formatter;

use crate::*;
use mqttrs::{ConnectReturnCode, Packet, Pid, QosPid, Suback, Unsubscribe};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec;

impl Client {
    pub async fn new(
        connect: mqttrs::Connect, broker: MqttBroker,
    ) -> MQTTResult<(
        Client,
        ClientId,
        Option<Message>,
        MessageConsumer,
        MessageProducer,
        UnboundedReceiverStream<Command>,
    )> {
        let client_id = ClientId::from(connect.client_id);
        // let will = connect.will().map(|msg| Message::new(0, msg.0.to_string(), 0, bytes::Bytes::from(msg.1)));
        let will = connect.last_will.map(|msg| Message::new(0, msg.topic.to_string(), msg.qos as u8, Bytes::new()));

        //Create channels
        let (tx, rx) = sync::mpsc::unbounded_channel();
        let (controller, commands) = sync::mpsc::unbounded_channel();

        //Register client in the broker
        let consumer = MessageConsumer::new(client_id.clone(), tx);
        let producer = MessageProducer::new(client_id.clone(), rx);

        let id = client_id.clone();
        let last_received_packet = SystemTime::now();
        let connected_on = SystemTime::now();

        let client = Client {
            broker,
            id,
            last_received_packet,
            connected_on,
            controller,
        };

        Ok((client, client_id, will, consumer, producer, UnboundedReceiverStream::new(commands)))
    }

    pub async fn process(self: &Self, packet: mqttrs::Packet) -> MQTTResult<Option<Ack>> {
        let broker = &self.broker;
        let client_id = self.id.clone();

        match packet {
            mqttrs::Packet::Subscribe(subscribe) => {
                let topics: Vec<(Topic, Qos)> = subscribe
                    .topics
                    .iter()
                    .map(|sub_topic| (sub_topic.topic_path.to_string(), sub_topic.qos as u8))
                    .collect();
                let packet_id = subscribe.pid.get();

                let result = broker.lock().await.subscribe(&client_id, topics);
                return result.map(|sub_results| Some(Ack::Subscribe(packet_id, sub_results)));
            }
            mqttrs::Packet::Unsubscribe(unsubscribe) => {
                let packet_id = unsubscribe.pid.get();
                let topics: Vec<Topic> = unsubscribe.topics.iter().map(|tf| tf.to_string()).collect();

                let result = broker.lock().await.unsubscribe(&self.id, topics);
                return result.map(|_| Some(Ack::Unsubscribe(packet_id)));
            }
            mqttrs::Packet::Publish(publish) => {
                let (qos, packet_id) = (publish.qospid.qos(), publish.qospid.pid());

                let topic_name = publish.topic_name;
                let bytes = Bytes::from(publish.payload);

                {
                    let mut broker = broker.lock().await;
                    broker.validate(&topic_name)?;
                    broker.publish(topic_name.to_string(), qos as u8, bytes)?;
                }

                //responses should be made in order of receiving
                if qos == mqttrs::QoS::AtLeastOnce {
                    return Ok(Some(Ack::Publish(packet_id.unwrap_or(Pid::new()).get())));
                } else if qos == mqttrs::QoS::ExactlyOnce {
                    //FIXME
                    //let response: Packet = PubAck { packet_id: publish.packet_id }.into();
                    //self.packets.send(response).await?;
                }
            }
            mqttrs::Packet::Puback(puback) => {
                broker.lock().await.acknowledge(puback.get())?;
            }
            mqttrs::Packet::Pingreq => {
                //FIXME implement connection timeout
                return Ok(Some(Ack::Ping));
            }
            mqttrs::Packet::Connect(_) => {
                error!("disconnecting client (duplicated CONNECT)");
            }
            mqttrs::Packet::Disconnect => {
                broker.lock().await.disconnect_cleanly(&client_id);
                if self.controller.send(Command::DISCONNECT).is_err() {
                    panic!("unable to properly disconnect client");
                }
            }
            packet_type => {
                panic!("Unknown packet type: {:?}", packet_type);
            }
        }
        Ok(None)
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{{clientId = {}}}", self.id)
    }
}

impl MessageConsumer {
    pub fn new(client_id: ClientId, tx: Tx) -> Self {
        let connected_on = SystemTime::now();
        MessageConsumer { tx, client_id, connected_on }
    }
}

impl Publisher for MessageConsumer {
    fn send(&self, message: Message) -> MQTTResult<()> {
        if self.tx.is_closed() {
            error!("Publishing to already closed client {:?}", self.client_id);
            return Err(MQTTError::ClosedClient(self.client_id.clone()));
        }

        let message = mqttrs::Publish {
            dup: false,
            qospid: MessageConsumer::qos_pid(message.qos, message.id as u16),
            retain: false,
            topic_name: message.topic,
            payload: message.payload.to_vec(),
        };

        self.tx.send(message.into()).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn ack(&self, ack: Ack) -> Result<(), MQTTError> {
        let packet: mqttrs::Packet = match ack {
            Ack::Connect => mqttrs::Connack {
                session_present: false,
                code: ConnectReturnCode::Accepted,
            }
            .into(),
            Ack::Publish(packet_id) => mqttrs::Packet::Puback(MessageConsumer::packet_id(packet_id)).into(),
            Ack::Subscribe(packet_id, sub_results) => {
                mqttrs::Packet::Suback(Suback {
                    pid: MessageConsumer::packet_id(packet_id),
                    //FIXME: fix subqos
                    return_codes: vec![],
                })
                .into()
            }
            Ack::Unsubscribe(packet_id) => mqttrs::Packet::Unsubscribe(Unsubscribe {
                pid: MessageConsumer::packet_id(packet_id),
                //FIXME: fix subqos
                topics: vec![],
            })
            .into(),
            Ack::Ping => mqttrs::Packet::Pingresp.into(),
        };
        self.tx.send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn disconnect(&self) {
        if self.tx.send(mqttrs::Packet::Disconnect.into()).is_err() {
            error!("failed to send DISCONNECT")
        }
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{{client_id = {}, connected_on: {:?}}}", self.client_id, self.connected_on)
    }
}

impl MessageProducer {
    pub fn new(client_id: ClientId, rx: Rx) -> Self {
        MessageProducer { rx, client_id }
    }

    pub async fn forward_to<W>(mut self, write: W, commands: mpsc::UnboundedSender<Command>)
    where
        W: AsyncWrite + Unpin,
        FramedWrite<W, PacketsCodec>: Sink<Packet>,
        <FramedWrite<W, PacketsCodec> as Sink<Packet>>::Error: fmt::Display,
    {
        let decoder = PacketsCodec::new();
        let mut packets = codec::FramedWrite::new(write, decoder);

        while let Some(packet) = self.rx.recv().await {
            if let Packet::Disconnect = packet {
                debug!("closing the connection");
                self.send_disconnect_cmd(commands);
                return;
            }

            match packets.send(packet).await {
                Ok(_) => {
                    debug!("sent packet to client {:?}", self.client_id)
                }
                Err(error) => {
                    error!(reason = % error, "error sending to client");
                    self.send_disconnect_cmd(commands);
                    return;
                }
            }
        }
    }

    fn send_disconnect_cmd(mut self, commands: UnboundedSender<Command>) {
        if commands.send(Command::DISCONNECT).is_ok() {
            self.rx.close();
        } else {
            error!("unable to disconnect the client")
        }
    }
}

impl Display for MessageProducer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}

impl MessageConsumer {
    fn packet_id(packet_id: u16) -> Pid {
        Pid::try_from(packet_id).unwrap_or(Pid::new())
    }
}

impl MessageConsumer {
    fn qos_pid(qos: u8, pid: PacketId) -> QosPid {
        match qos {
            0 => mqttrs::QosPid::AtMostOnce,
            1 => mqttrs::QosPid::AtLeastOnce(Pid::try_from(pid).unwrap()),
            2 => mqttrs::QosPid::ExactlyOnce(Pid::try_from(pid).unwrap()),
            _ => {
                panic!("invalid qos")
            }
        }
    }
}
