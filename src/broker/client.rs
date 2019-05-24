use std::convert::TryInto;
use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use futures::sync::mpsc;
use futures::{task, Async, Future, Stream};

use broker::{Broker, Client, Puback, Publish, Subscribe};
use mqtt::{Packet, PacketType, Packets};
use MQTTError;

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

        self.packets.poll_flush()?;

        while let Async::Ready(Some(packet)) = self.incoming.poll().map_err(|_| "something went wrong with incoming")? {
            self.packets.buffer(packet);
        }

        let mut broker = self.broker.lock().expect("missing broker");
        while let Async::Ready(packet) = self.packets.poll().map_err(|_| "something went wrong with packets")? {
            //check for incoming flood of packets?

            if let Some(packet) = packet {
                info!("Handling packet: {} by client: {}", packet, self);
                self.last_received_packet = SystemTime::now();

                match packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.try_into()?;
                        let packet_id = subscribe.packet_id;

                        if let Ok(results) = broker.subscribe(&self.client_id, subscribe) {
                            let response = Packet::suback(packet_id, &results);
                            self.packets.buffer(response);
                        } else {
                            info!("Disconnecting client (malformed or invalid subscribe): {}", self);
                            return Ok(Async::Ready(()));
                        }
                    }
                    PacketType::PUBLISH => {
                        let publish: Publish = packet.try_into()?;
                        if publish.qos != 0 {
                            let response = Packet::puback(publish.packet_id);
                            self.packets.buffer(response);
                        }

                        broker.publish(publish)?;
                    }
                    PacketType::PUBACK => {
                        let puback: Puback = packet.try_into()?;

                        broker.acknowledge(puback.packet_id)?;
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
        return Ok(Async::NotReady);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let client_id = self.client_id.to_owned();
        info!("Dropped client");
        self.broker.lock().unwrap().disconnect(client_id);
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Client(clientId: {}, addr: {})", self.client_id, self.addr)
    }
}

impl Client {
    pub fn new(packet: Packet, broker: Arc<Mutex<Broker>>, packets: Packets) -> Client {
        let addr = packets.socket.peer_addr().unwrap();
        let (outgoing, incoming) = mpsc::unbounded();

        let connect = packet.try_into().expect("Malformed connect");

        let client_id = broker.lock().expect("Missing broker").connect(connect, outgoing);

        Client {
            client_id,
            addr,
            packets,
            incoming,
            broker,
            disconnected: false,
            last_received_packet: SystemTime::now(),
        }
    }

    fn buffer(&mut self, packet: Packet) {
        self.packets.buffer(packet);
    }

    pub fn send_connack(&mut self) {
        self.buffer(Packet::connack());
    }

    fn verify_timeout(&mut self) -> bool {
        SystemTime::now()
            .duration_since(self.last_received_packet)
            .map(|duration| duration.as_secs() >= 60)
            .unwrap_or(false)
    }
}
