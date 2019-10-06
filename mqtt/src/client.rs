use std::convert::TryInto;
use std::fmt;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::{mpsc, Arc};
use std::task::Context;
use std::time::SystemTime;

use futures::{Poll, SinkExt, Stream, StreamExt};
use tokio::codec::Framed;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use broker::{Broker, ClientId};
use packets::{ConnAck, Connect, MQTTError, Packet, PacketType, PingResp, PubAck, Publish, SubAck, Subscribe, Unsubscribe};

use crate::{Client, Message, PacketsCodec};

pub type FramedPackets = Framed<TcpStream, PacketsCodec>;

impl Client {
    pub async fn new(broker: Arc<Mutex<Broker>>, connect: Connect, mut packets: FramedPackets) -> io::Result<(Self, ClientId)> {
        let (outgoing, incoming) = mpsc::channel();

        let client_id = connect.client_id.unwrap();

        //Register client in the broker
        broker.lock().await.register(client_id.clone().as_str(), outgoing)?;

        //Respond with CONNACK
        let conn_ack: Packet = ConnAck::default().into();
        packets.send(conn_ack).await?;

        let client = Client {
            client_id: client_id.clone(),
            addr: packets.get_ref().peer_addr()?,
            disconnected: false,
            packets,
            incoming,
            broker,
            last_received_packet: SystemTime::now(),
        };

        Ok((client, client_id))
    }

    pub async fn poll(mut self: Client) -> io::Result<()> {
        while let Some(result) = self.next().await {
            match result {
                Ok(Message::Received(packet)) => {
                    match &packet.packet_type {
                        PacketType::SUBSCRIBE => {
                            let subscribe: Subscribe = packet.try_into()?;
                            let packet_id = subscribe.packet_id;

                            let mut broker = self.broker.lock().await;
                            if let Ok(sub_results) = broker.subscribe(&self.client_id, subscribe) {
                                let response: Packet = SubAck { packet_id, sub_results }.into();

                                self.packets.send(response).await?;
                            }
                        }
                        PacketType::UNSUBSCRIBE => {
                            let unsubscribe: Unsubscribe = packet.try_into()?;

                            let mut broker = self.broker.lock().await;
                            let _unsub_result = broker.unsubscribe(&self.client_id, unsubscribe);
                        }
                        PacketType::PUBLISH => {
                            let publish: Publish = packet.try_into()?;
                            let packet_id = publish.packet_id;
                            let qos = publish.qos;

                            let mut broker = self.broker.lock().await;
                            broker.validate(&publish)?;
                            broker.publish(publish)?;

                            if qos == 1 {
                                let response: Packet = PubAck { packet_id }.into();
                                self.packets.send(response).await?;
                            } else if qos == 2 {
                                //                                let response: Packet = PubAck { packet_id: publish.packet_id }.into();
                                //                                self.packets.send(response).await?;
                            }
                        }
                        PacketType::PUBACK => {
                            let puback: PubAck = packet.try_into()?;

                            let mut broker = self.broker.lock().await;
                            broker.acknowledge(puback.packet_id)?;
                        }
                        PacketType::PINGREQ => {
                            let ping_resp: Packet = PingResp::default().into();

                            self.packets.send(ping_resp).await?;
                        }
                        PacketType::CONNECT => {
                            error!("Disconnecting client (duplicated CONNECT): {}", self);
                            return Ok(());
                        }
                        PacketType::DISCONNECT => {
                            return Ok(());
                        }
                        packet_type => {
                            panic!("Unknown packet type: {:?}", packet_type);
                        }
                    }
                }
                Ok(Message::Broadcast(packet)) => {
                    self.packets.send(packet).await?;
                }
                Err(e) => {
                    error!("an error occurred while processing messages for {}; error = {:?}", self.client_id, e);
                }
            }
        }
        Ok(())
    }
}

impl Stream for Client {
    type Item = Result<Message, MQTTError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let incoming = self.incoming.try_recv();
        if incoming.is_ok() {
            return Poll::Ready(Some(Ok(Message::Broadcast(incoming.unwrap().into()))));
        }

        //        if let Poll::Ready(Some(v)) = self.incoming.poll_next_unpin(cx) {
        //            return Poll::Ready(Some(Ok(Message::Broadcast(v.into()))));
        //        }

        let result: Option<_> = futures::ready!(self.packets.poll_next_unpin(cx));

        Poll::Ready(match result {
            Some(Ok(message)) => Some(Ok(Message::Received(message))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        })
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Client(clientId: {}, addr: {})", self.client_id, self.addr)
    }
}
