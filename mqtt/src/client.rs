use core::MQTTError;
use std::fmt;
use std::fmt::Formatter;

use crate::*;
use mqtt_proto::control::ConnectReturnCode;
use mqtt_proto::packet::suback::SubscribeReturnCode;
use mqtt_proto::packet::*;
use mqtt_proto::{QualityOfService, TopicName};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::codec;

impl Client {
    pub async fn new(
        connect: mqtt_proto::packet::ConnectPacket, broker: MqttBroker,
    ) -> MQTTResult<(
        Client,
        ClientId,
        Option<Message>,
        MessageConsumer,
        MessageProducer,
        UnboundedReceiverStream<Command>,
    )> {
        let client_id = ClientId::from(connect.client_identifier());
        // let will = connect.will().map(|msg| Message::new(0, msg.0.to_string(), 0, bytes::Bytes::from(msg.1)));
        let will = connect.will().map(|msg| Message::new(0, msg.0.to_string(), 0, Bytes::new()));

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

    pub async fn process(self: &Self, packet: VariablePacket) -> MQTTResult<Option<Ack>> {
        let broker = &self.broker;
        let client_id = self.id.clone();

        match packet {
            VariablePacket::SubscribePacket(subscribe) => {
                let topics: Vec<(Topic, Qos)> = subscribe.subscribes().iter().map(|(tf, qos)| (tf.to_string(), *qos as u8)).collect();
                let packet_id = subscribe.packet_identifier();

                let result = broker.lock().await.subscribe(&client_id, topics);
                return result.map(|sub_results| Some(Ack::Subscribe(packet_id, sub_results)));
            }
            VariablePacket::UnsubscribePacket(unsubscribe) => {
                let packet_id = unsubscribe.packet_identifier();
                let topics: Vec<Topic> = unsubscribe.subscribes().iter().map(|tf| tf.to_string()).collect();

                let result = broker.lock().await.unsubscribe(&self.id, topics);
                return result.map(|_| Some(Ack::Unsubscribe(packet_id)));
            }
            VariablePacket::PublishPacket(publish) => {
                let (qos, packet_id) = publish.qos().split();

                let topic_name = publish.topic_name();
                let bytes = Bytes::copy_from_slice(publish.payload());

                {
                    let mut broker = broker.lock().await;
                    broker.validate(&topic_name)?;
                    broker.publish(topic_name.to_string(), qos as u8, bytes)?;
                }

                //responses should be made in order of receiving
                if qos == QualityOfService::Level1 {
                    return Ok(Some(Ack::Publish(packet_id.unwrap())));
                } else if qos == QualityOfService::Level2 {
                    //FIXME
                    //let response: Packet = PubAck { packet_id: publish.packet_id }.into();
                    //self.packets.send(response).await?;
                }
            }
            VariablePacket::PubackPacket(puback) => {
                broker.lock().await.acknowledge(puback.packet_identifier() as u16)?;
            }
            VariablePacket::PingreqPacket(_) => {
                //FIXME implement connection timeout
                return Ok(Some(Ack::Ping));
            }
            VariablePacket::ConnectPacket(_) => {
                error!("disconnecting client (duplicated CONNECT)");
            }
            VariablePacket::DisconnectPacket(_) => {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        let name = TopicName::new(message.topic).unwrap();
        let message = PublishPacket::new(
            name,
            QoSWithPacketIdentifier::new(QualityOfService::Level1, message.id as u16),
            message.payload.to_vec(),
        );
        self.tx.send(message.into()).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn ack(&self, ack: Ack) -> Result<(), MQTTError> {
        let packet: VariablePacket = match ack {
            Ack::Connect => ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted).into(),
            Ack::Publish(packet_id) => PubackPacket::new(packet_id).into(),
            Ack::Subscribe(packet_id, sub_results) => {
                //FIXME: fix subqos
                SubackPacket::new(packet_id, sub_results.iter().map(|sub_qos| SubscribeReturnCode::MaximumQoSLevel2).collect()).into()
            }
            Ack::Unsubscribe(packet_id) => UnsubackPacket::new(packet_id).into(),
            Ack::Ping => PingrespPacket::new().into(),
        };
        self.tx.send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn disconnect(&self) {
        if self.tx.send(DisconnectPacket::new().into()).is_err() {
            error!("failed to send DISCONNECT")
        }
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        FramedWrite<W, MqttCodec>: Sink<VariablePacket>,
        <FramedWrite<W, MqttCodec> as Sink<VariablePacket>>::Error: fmt::Display,
    {
        let decoder = mqtt_proto::packet::MqttCodec::new();
        let mut packets = codec::FramedWrite::new(write, decoder);

        while let Some(packet) = self.rx.recv().await {
            if let VariablePacket::DisconnectPacket(_) = packet {
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}
