use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;

use futures::Async;
use futures::Future;
use futures::Stream;
use futures::sync::mpsc;

use mqtt::*;
use mqtt::broker::Publish;
use mqtt::broker::Subscribe;

impl Future for Client {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if self.disconnected { return Ok(Async::Ready(())); }

        self.handle_incoming_packets()?;

        self.packets.poll_flush()?;

        while let Async::Ready(packet) = self.packets.poll()? {
            if let Some(packet) = packet {
                info!("Handling new packet: {}", packet);
                let mut broker = self.broker.lock().expect("missing broker");

                match packet.packet_type {
                    PacketType::SUBSCRIBE => {
                        let subscribe: Subscribe = packet.into();
                        let packet_id = subscribe.packet_id;
                        if broker.subscribe(&self, subscribe).is_ok() {
                            self.packets.buffer(Packet::suback(packet_id));
                        } else {
                            return Ok(Async::Ready(()));
                        }
                    }
                    PacketType::PUBLISH => {
                        let publish: Publish = packet.into();
                        if publish.qos != 0 {
                            self.packets.buffer(Packet::puback(publish.packet_id));
                        }

                        broker.publish(publish).is_ok();
                    }
                    PacketType::PINGREQ => {
                        self.packets.buffer(Packet::pingres())
                    }
                    PacketType::DISCONNECT => {
                        info!("Client disconnected: {:?}", self);
                        return Ok(Async::Ready(()));
                    }
                    packet => error!("Unsupported packet: {:?}", packet),
                }
            } else {
                info!("Disconnecting misbehaving client: {}", self);
                return Ok(Async::Ready(()));
            }
        }

        Ok(Async::NotReady)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let client_id = self.client_id.to_owned();
        info!("Disconnecting: {}", self);
        self.broker.lock().unwrap()
            .disconnect(client_id);
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Client(clientId: {}, addr: {})", self.client_id, self.addr)
    }
}

impl Client {
    pub fn new(connect: Packet, broker: Arc<Mutex<Broker>>, packets: Packets) -> Client {
        let addr = packets.socket.peer_addr().unwrap();
        let (outgoing, incoming) = mpsc::unbounded();

        let client_id = broker.lock().expect("Missing broker").connect(connect.into(), outgoing);

        let mut client = Client {
            client_id,
            addr,
            packets,
            incoming,
            broker,
            disconnected: false,
        };

        client.send_connack();
        client
    }

    fn buffer(&mut self, packet: Packet) {
        self.packets.buffer(packet);
    }

    fn send_connack(&mut self) {
        self.buffer(Packet::connack());
    }

    fn handle_incoming_packets(&mut self) -> Result<(), io::Error> {
        let incoming_poll = self.incoming.poll()
            .map_err(|_| io::Error::new(ErrorKind::Other, "something went wrong"))?;

        if let Async::Ready(Some(packet)) = incoming_poll {
            self.packets.buffer(packet);
        }

        Ok(())
    }
}