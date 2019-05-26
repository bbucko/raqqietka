use std::convert::TryInto;
use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

use futures::{Async, Future, Stream, task};
use futures::sync::mpsc;

use broker::{Broker, Client, Connect, Puback, Publish, Subscribe};
use mqtt::{Packet, Packets, PacketType, Tx};
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

        //receive messages from broker
        loop {
            match self.incoming.poll().unwrap() {
                Async::Ready(Some(packet)) => self.packets.buffer(packet),
                _ => break
            }
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

                        let subscription_result = self.broker.lock().map(|mut broker| broker.subscribe(&self.client_id, subscribe)).map_err(MQTTError::from);

                        if let Ok(results) = subscription_result? {
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
                        self.broker.lock().map_err(MQTTError::from).and_then(|mut broker| broker.publish(publish))?;
                    }
                    PacketType::PUBACK => {
                        let puback: Puback = packet.try_into()?;

                        self.broker.lock().map_err(MQTTError::from).and_then(|mut broker| broker.acknowledge(puback.packet_id))?;
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
    pub fn new(packet: Packet, broker: Arc<Mutex<Broker>>, packets: Packets) -> Result<(Client, Tx), MQTTError> {
        let client_id = packet.try_into()
            .map(|it: Connect| it.client_id)?
            .ok_or(format!("missing client_id"))?;

        let addr = packets.socket.peer_addr()
            .map_err(|e| format!("missing addr: {}", e))?;

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
