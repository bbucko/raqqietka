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
        let tx_publisher = MessageConsumer::new(client_id.clone(), tx);
        let rx_publisher = MessageProducer::new(client_id.clone(), rx);

        let client = Client {
            broker,
            id: client_id.clone(),
            disconnected: false,
            last_received_packet: SystemTime::now(),
        };

        Ok((client, client_id, connect.will.map(|msg| msg.into()), tx_publisher, rx_publisher))
    }

    pub async fn process(self: &Self, result: Result<Packet, MQTTError>) -> MQTTResult<Option<Ack>> {
        let broker = &self.broker;
        let client_id = self.id.clone();

        match result {
            Ok(packet) => {
                match &packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let topics = subscribe.topics;

                        let result = broker.lock().await.subscribe(&self.id, topics);
                        if let Ok(sub_results) = result {
                            return Ok(Some(Ack::Subscribe(subscribe.packet_id, sub_results)));
                        }
                    }
                    PacketType::UNSUBSCRIBE => {
                        let unsubscribe: Unsubscribe = packet.try_into()?;

                        let _result = broker.lock().await.unsubscribe(&self.id, unsubscribe.topics);
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
                        broker.lock().await.disconnect_cleanly(client_id);
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
