use std::convert::TryInto;
use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use futures::sync::mpsc;
use futures::{task, Async, Future, Stream};

use crate::broker::*;
use crate::MQTTError;

const CLIENT_ID_MAX_LENGTH: u8 = 64;

impl Future for Client {
    type Item = ();
    type Error = MQTTError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.disconnected {
            info!("Disconnecting client (unknown reason): {}", self);
            return Ok(Async::Ready(()));
        }

        if self.verify_timeout() {
            info!("Disconnecting client (timeout): {}", self);
            return Ok(Async::Ready(()));
        }

        //receive messages from broker
        while let Async::Ready(Some(packet)) = self.incoming.poll().unwrap() {
            self.packets.buffer(packet);
        }

        self.packets.poll_flush()?;

        //read lines from the broker
        while let Async::Ready(packet) = self.packets.poll()? {
            //TODO add flood prevention
            if let Some(packet) = packet {
                info!("Handling packet: {} by client: {}", packet, self);
                self.last_received_packet = SystemTime::now();

                match packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let packet_id = subscribe.packet_id;

                        let subscription_result = self
                            .broker
                            .lock()
                            .map(|mut broker| broker.subscribe(&self.client_id, subscribe))
                            .map_err(MQTTError::from);

                        if let Ok(sub_results) = subscription_result? {
                            let response = Suback { packet_id, sub_results }.into();
                            self.packets.buffer(response);
                        } else {
                            info!("Disconnecting client (malformed or invalid subscribe): {}", self);
                            return Ok(Async::Ready(()));
                        }
                    }
                    PacketType::UNSUBSCRIBE => {
                        let unsubscribe: Unsubscribe = packet.try_into()?;
                        let _unsub_result = self
                            .broker
                            .lock()
                            .map(|mut broker| broker.unsubscribe(&self.client_id, unsubscribe))
                            .map_err(MQTTError::from);
                    }
                    PacketType::PUBLISH => {
                        let mut broker = self.broker.lock().map_err(MQTTError::from)?;

                        let publish: Publish = packet.try_into()?;

                        broker.validate(&publish)?;

                        if publish.qos != 0 {
                            let response: Packet = Puback { packet_id: publish.packet_id }.into();
                            self.packets.buffer(response);
                            self.packets.poll_flush()?;
                        }

                        broker.publish(publish)?;
                    }
                    PacketType::PUBACK => {
                        let puback: Puback = packet.try_into()?;

                        self.broker
                            .lock()
                            .map_err(MQTTError::from)
                            .and_then(|mut broker| broker.acknowledge(puback.packet_id))?;
                    }
                    PacketType::PINGREQ => self.packets.buffer(Packet::pingres()),
                    PacketType::CONNECT => {
                        info!("Disconnecting client (duplicated CONNECT): {}", self);
                        return Ok(Async::Ready(()));
                    }
                    PacketType::DISCONNECT => {
                        info!("Disconnecting client (disconnect): {}", self);
                        return Ok(Async::Ready(()));
                    }
                    packet => error!("Unsupported packet: {:?}", packet),
                }
            } else {
                info!("Disconnecting client (malformed packet?): {}", self);
                return Ok(Async::Ready(()));
            }
        }

        //hmm.. yielding?
        task::current().notify();
        Ok(Async::NotReady)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let client_id = self.client_id.to_owned();
        info!("Dropped client");

        match self.broker.lock() {
            Ok(mut broker) => broker.disconnect(client_id),
            Err(e) => error!("error locking mutex: {}", e),
        }
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Client(clientId: {}, addr: {})", self.client_id, self.addr)
    }
}

impl Client {
    pub fn new(packet: Packet, broker: Arc<Mutex<Broker>>, packets: Packets) -> Result<(Client, Tx), MQTTError> {
        let client_id = packet.try_into().map(|it: Connect| it.client_id)?.ok_or_else(|| "missing client_id")?;
        if client_id.is_empty() || client_id.len() > usize::from(CLIENT_ID_MAX_LENGTH) {
            return Err(format!("malformed client_id invalid length: {}", client_id.len()).into());
        }

        if !client_id.chars().all(char::is_alphanumeric) {
            return Err(format!("malformed client_id invalid characters: {}", client_id).into());
        }

        let addr = packets.socket.peer_addr().map_err(|e| format!("missing addr: {}", e))?;

        let (outgoing, incoming) = mpsc::unbounded();

        let client = Client {
            client_id,
            addr,
            packets,
            incoming,
            broker,
            disconnected: false,
            last_received_packet: SystemTime::now(),
        };

        Ok((client, outgoing))
    }

    pub fn send_connack(&mut self) {
        self.packets.buffer(Packet::connack());
    }

    fn verify_timeout(&mut self) -> bool {
        SystemTime::now()
            .duration_since(self.last_received_packet)
            .map(|duration| duration.as_secs() >= 60)
            .unwrap_or(false)
    }
}
