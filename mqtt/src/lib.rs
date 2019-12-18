#![warn(rust_2018_idioms)]

use std::convert::TryInto;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::SystemTime;

use broker::Broker;
use core::*;
use futures::prelude::*;
use futures::SinkExt;
use num_traits;
use tokio::io::AsyncWrite;
use tokio::sync;
use tokio_util::codec::FramedWrite;
use tracing::{debug, error, info};

mod codec;

pub type MqttBroker = Arc<sync::Mutex<Broker<MessageConsumer>>>;

pub type Tx = sync::mpsc::UnboundedSender<Packet>;
pub type Rx = sync::mpsc::UnboundedReceiver<Packet>;

#[derive(Debug, Default)]
pub struct PacketsCodec {}

#[derive(Debug)]
pub struct Client {
    broker: MqttBroker,
    id: ClientId,
    disconnected: bool,
    last_received_packet: SystemTime,
}

impl Client {
    pub async fn new(connect: Connect, broker: MqttBroker) -> MQTTResult<(Client, ClientId, Option<ApplicationMessage>, MessageConsumer, MessageProducer)> {
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

    pub async fn process(self: &Self, result: Result<Packet, MQTTError>) -> MQTTResult<Option<Packet>> {
        let broker = &self.broker;
        let client_id = self.id.clone();

        match result {
            Ok(packet) => {
                match &packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let packet_id = subscribe.packet_id;
                        let topics = subscribe.topics;

                        let result = broker.lock().await.subscribe(&self.id, topics);
                        if let Ok(sub_results) = result {
                            let response: Packet = SubAck { packet_id, sub_results }.into();
                            return Ok(Some(response));
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
                            broker.validate(&publish)?;
                            broker.publish(publish)?;
                        }

                        //responses should be made in order of receiving
                        if qos == 1 {
                            let response: Packet = PubAck { packet_id }.into();
                            return Ok(Some(response));
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
                        let response: Packet = PingResp::default().into();
                        return Ok(Some(response));
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

#[derive(Debug, Clone)]
pub struct MessageConsumer {
    client_id: ClientId,
    tx: Tx,
}

impl MessageConsumer {
    pub fn new(client_id: ClientId, tx: Tx) -> Self {
        MessageConsumer { tx, client_id }
    }
}

impl Publisher for MessageConsumer {
    fn send(&self, packet: Packet) -> MQTTResult<()> {
        self.tx.clone().send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}

#[derive(Debug)]
pub struct MessageProducer {
    client_id: ClientId,
    rx: Rx,
}

impl MessageProducer {
    pub fn new(client_id: ClientId, rx: Rx) -> Self {
        MessageProducer { rx, client_id }
    }

    pub fn close(&mut self) {
        info!("closing channel");
        self.rx.close();
    }

    pub async fn forward_to<W>(mut self, write: W)
    where
        W: AsyncWrite + Unpin,
        FramedWrite<W, PacketsCodec>: Sink<Packet>,
        <FramedWrite<W, PacketsCodec> as Sink<Packet>>::Error: fmt::Display,
    {
        let mut lines = FramedWrite::new(write, PacketsCodec::new());

        while let Some(msg) = self.rx.next().await {
            match lines.send(msg).await {
                Ok(_) => {
                    //
                }
                Err(error) => {
                    error!(reason = %error, "error sending to client");
                    self.close();
                    return;
                }
            }
        }

        // The client has disconnected, we can stop forwarding.
        debug!("client is no longer connected");
        self.close();
    }
}

impl Display for MessageProducer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}
