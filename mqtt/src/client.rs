#![warn(rust_2018_idioms)]

use std::fmt;
use std::fmt::Formatter;

use core::MQTTError;

use crate::*;

impl Client {
    pub async fn new(connect: Connect, broker: MqttBroker) -> MQTTResult<(Client, ClientId, Option<Message>, MessageConsumer, MessageProducer)> {
        let client_id = connect.client_id.ok_or_else(|| MQTTError::ClientError("missing clientId".to_string()))?;

        //Create channels
        let (tx, rx) = sync::mpsc::unbounded_channel();

        //Register client in the broker
        let consumer = MessageConsumer::new(client_id.clone(), tx);
        let producer = MessageProducer::new(client_id.clone(), rx);

        let client = Client {
            broker,
            id: client_id.clone(),
            disconnected: false,
            last_received_packet: SystemTime::now(),
        };

        Ok((client, client_id, connect.will.map(|msg| msg.into()), consumer, producer))
    }

    pub async fn process(self: &Self, packet: Result<Packet, MQTTError>) -> MQTTResult<Option<Ack>> {
        let broker = &self.broker;
        let client_id = self.id.clone();

        match packet {
            Ok(packet) => {
                match &packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let topics = subscribe.topics;
                        let packet_id = subscribe.packet_id;

                        let result = broker.lock().await.subscribe(&client_id, topics);
                        return result.map(|sub_results| Some(Ack::Subscribe(packet_id, sub_results)));
                    }
                    PacketType::UNSUBSCRIBE => {
                        let unsubscribe: Unsubscribe = packet.try_into()?;
                        let packet_id = unsubscribe.packet_id;

                        let result = broker.lock().await.unsubscribe(&self.id, unsubscribe.topics);
                        return result.map(|_| Some(Ack::Unsubscribe(packet_id)));
                    }
                    PacketType::PUBLISH => {
                        let publish: Publish = packet.try_into()?;
                        let packet_id = publish.packet_id;
                        let qos = publish.qos;

                        {
                            let mut broker = broker.lock().await;
                            broker.validate(&publish.topic)?;
                            broker.publish(publish.topic, publish.qos, publish.payload)?;
                        }

                        //responses should be made in order of receiving
                        if qos == 1 {
                            return Ok(Some(Ack::Publish(packet_id)));
                        } else if qos == 2 {
                            //FIXME
                            //let response: Packet = PubAck { packet_id: publish.packet_id }.into();
                            //self.packets.send(response).await?;
                        }
                    }
                    PacketType::PUBACK => {
                        let puback: PubAck = packet.try_into()?;
                        broker.lock().await.acknowledge(puback.packet_id)?;
                    }
                    PacketType::PINGREQ => {
                        //FIXME implement connection timeout
                        return Ok(Some(Ack::Ping));
                    }
                    PacketType::CONNECT => {
                        error!("disconnected client (duplicated CONNECT)");
                    }
                    PacketType::DISCONNECT => {
                        broker.lock().await.disconnect_cleanly(&client_id);
                    }
                    packet_type => {
                        panic!("Unknown packet type: {:?}", packet_type);
                    }
                }
            }

            Err(e) => {
                error!("an error occurred while processing messages for {}; error = {:?}", self.id, e);
                return Err(MQTTError::OtherError(e.to_string()));
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
        MessageConsumer { tx, client_id }
    }
}

impl Publisher for MessageConsumer {
    fn send(&self, message: Message) -> MQTTResult<()> {
        self.tx.send(message.into()).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn ack(&self, ack: Ack) -> Result<(), MQTTError> {
        let packet = match ack {
            Ack::Connect => ConnAck::default().into(),
            Ack::Publish(packet_id) => PubAck::new(packet_id).into(),
            Ack::Subscribe(packet_id, sub_results) => SubAck::new(packet_id, sub_results).into(),
            Ack::Unsubscribe(packet_id) => UnsubAck::new(packet_id).into(),
            Ack::Ping => PingResp::default().into(),
        };
        self.tx.send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn disconnect(&self) {
        if self.tx.send(Disconnect::default().into()).is_err() {
            error!("unable to disconnect")
        }
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}

impl MessageProducer {
    pub fn new(client_id: ClientId, rx: Rx) -> Self {
        MessageProducer { rx, client_id }
    }

    pub async fn forward_to<W>(mut self, write: W)
    where
        W: AsyncWrite + Unpin,
        FramedWrite<W, PacketsCodec>: Sink<Packet>,
        <FramedWrite<W, PacketsCodec> as Sink<Packet>>::Error: fmt::Display,
    {
        let mut lines = FramedWrite::new(write, PacketsCodec::new());

        while let Some(msg) = self.rx.next().await {
            if msg.packet_type == PacketType::DISCONNECT {
                //Do not forward. Just close the connection.
                debug!("closing the connection");
                if lines.close().await.is_err() {
                    error!("unable to disconnect")
                }
                return;
            }

            match lines.send(msg).await {
                Ok(_) => {
                    //
                }
                Err(error) => {
                    error!(reason = %error, "error sending to client");
                    return;
                }
            }
        }

        // The client has disconnected, we can stop forwarding.
        debug!("client is no longer connected");
    }
}

impl Display for MessageProducer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}
