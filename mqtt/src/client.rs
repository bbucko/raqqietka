use std::convert::TryInto;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::error;

use broker::{Broker, ClientId};
use core::{Connect, MQTTError, MQTTResult, Packet, PacketType, PingResp, PubAck, Publish, SubAck, Subscribe, Unsubscribe};

use crate::{Client, MessageProducer, MessageConsumer};

impl Client {
    pub async fn new(connect: Connect) -> MQTTResult<(Self, MessageConsumer, MessageProducer, ClientId)> {
        let client_id = connect.client_id.ok_or_else(|| MQTTError::ClientError("missing clientId".to_string()))?;

        //Create channels
        let (tx, rx) = mpsc::unbounded_channel();

        //Register client in the broker
        let tx_publisher = MessageConsumer::new(client_id.clone(), tx);
        let rx_publisher = MessageProducer::new(client_id.clone(), rx);

        let client = Client {
            id: client_id.clone(),
            disconnected: false,
            last_received_packet: SystemTime::now(),
        };

        Ok((client, tx_publisher, rx_publisher, client_id))
    }

    pub async fn process_packet(self: &Self, broker: Arc<Mutex<Broker>>, result: Result<Packet, MQTTError>) -> MQTTResult<Option<Packet>> {
        match result {
            Ok(packet) => {
                match &packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let packet_id = subscribe.packet_id;

                        let mut broker = broker.lock().await;
                        if let Ok(sub_results) = broker.subscribe(&self.id, subscribe) {
                            let response: Packet = SubAck { packet_id, sub_results }.into();
                            return Ok(Some(response));
                        }
                    }
                    PacketType::UNSUBSCRIBE => {
                        let unsubscribe: Unsubscribe = packet.try_into()?;

                        let mut broker = broker.lock().await;
                        let _unsub_result = broker.unsubscribe(&self.id, unsubscribe);
                    }
                    PacketType::PUBLISH => {
                        let publish: Publish = packet.try_into()?;
                        let packet_id = publish.packet_id;
                        let qos = publish.qos;

                        let mut broker = broker.lock().await;
                        broker.validate(&publish)?;

                        if qos == 1 {
                            let response: Packet = PubAck { packet_id }.into();
                            return Ok(Some(response));
                        } else if qos == 2 {
                            //let response: Packet = PubAck { packet_id: publish.packet_id }.into();
                            //self.packets.send(response).await?;
                        }

                        broker.publish(publish)?;
                    }
                    PacketType::PUBACK => {
                        let puback: PubAck = packet.try_into()?;

                        let mut broker = broker.lock().await;
                        broker.acknowledge(puback.packet_id)?;
                    }
                    PacketType::PINGREQ => {
                        let response: Packet = PingResp::default().into();

                        return Ok(Some(response));
                    }
                    PacketType::CONNECT => {
                        error!("Disconnecting client (duplicated CONNECT): {}", self);
                        return Ok(None);
                    }
                    PacketType::DISCONNECT => {
                        return Ok(None);
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
        write!(f, "Client(clientId = {})", self.id)
    }
}
