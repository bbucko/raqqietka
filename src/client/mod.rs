use broker::Broker;
use bytes::Bytes;
use codec::{MQTT as Codec, Packet};
use futures::sync::mpsc;
use self::handlers::{MQTTPacket, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io;
use tokio::prelude::*;

mod handlers;

type Tx = mpsc::UnboundedSender<Bytes>;
type Rx = mpsc::UnboundedReceiver<Bytes>;

static LINES_PER_TICK: usize = 10;

#[derive(Debug)]
pub struct Client {
    client_id: String,
    packets: Codec,
    addr: SocketAddr,
    rx: Rx,
    broker: Arc<Broker>,
}

impl Client {
    pub fn id(&self) -> &SocketAddr { &self.addr }

    pub fn new(packet: Packet, packets: Codec, broker: Arc<Broker>, addr: SocketAddr) -> Option<(Self, Tx)> {
        match handlers::connect(&packet.payload) {
            Ok(Some((client_id, _username, _password, _will))) => {
                let (tx, rx) = mpsc::unbounded();
                Some((
                    Self {
                        client_id,
                        packets,
                        rx,
                        broker,
                        addr,
                    },
                    tx,
                ))
            }
            _ => None,
        }
    }

    fn response(&self, msg: Vec<u8>) { self.broker.publish_message(self, msg); }
}

impl Future for Client {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        debug!("flush outbound queue: {:?}", self.rx);
        for i in 0..LINES_PER_TICK {
            match self.rx.poll().unwrap() {
                Async::Ready(Some(v)) => {
                    info!("sending packets: {:?}", v);
                    self.packets.buffer(&v);

                    //be good to others my dear future ;)
                    if i + 1 == LINES_PER_TICK {
                        task::current().notify();
                    }
                }
                _ => break,
            }
        }

        self.packets.poll_flush()?;

        while let Async::Ready(packet) = self.packets.poll()? {
            if let Some(packet) = packet {
                //handle various packets
                debug!(
                    "Received parsed packet: {:?}) :: {:?}; client: {:?}",
                    packet.packet_type, packet.payload, self.client_id
                );

                let rq = match packet.packet_type {
                    3 => handlers::publish(&packet.payload, packet.flags),
                    8 => handlers::subscribe(&packet.payload),
                    12 => handlers::pingreq(&packet.payload),
                    14 => handlers::disconnect(&packet.payload),
                    _ => return Ok(Async::Ready(())),
                }?;

                if let Some(rq) = rq {
                    info!("rq: {:?}", rq);
                    let rs = match rq.packet {
                        Type::PUBLISH(Some(packet_identifier), _topic, _qos_level, _payload) => MQTTPacket::puback(packet_identifier),
                        Type::PUBLISH(None, _topic, 0, _payload) => MQTTPacket::none(),
                        Type::SUBSCRIBE(packet_identifier, topics) => MQTTPacket::suback(packet_identifier, topics.iter().map(|topic| topic.1).collect()),
                        Type::PINGREQ => MQTTPacket::pingres(),
                        Type::DISCONNECT => MQTTPacket::none(),
                        _ => MQTTPacket::none(),
                    };

                    info!("rs: {:?}", rs);
                    match rs.packet {
                        Type::PUBACK(packet_identifier) => {
                            self.response(vec![0b0100_0000, 0b0000_0010, (packet_identifier >> 8) as u8, packet_identifier as u8]);
                        }
                        Type::SUBACK(packet_identifier, qos) => {
                            let mut payload = vec![0b1001_0000, 0b0000_0010, (packet_identifier >> 8) as u8, packet_identifier as u8];
                            payload.extend(qos);
                            self.response(payload);
                        }
                        Type::PINGRES => self.response(vec![0b1101_0000, 0b0000_0000]),
                        Type::NONE => return Ok(Async::NotReady),
                        Type::DISCONNECT => return Ok(Async::Ready(())),
                        _ => return Ok(Async::Ready(())),
                    }
                } else {
                    return Ok(Async::Ready(()));
                }
            } else {
                //abort, abort, abort
                return Ok(Async::Ready(()));
            }
        }
        //verify if packets returned NotReady
        Ok(Async::NotReady)
    }
}

impl Drop for Client {
    fn drop(&mut self) { Broker::unregister(self, self.broker.clone()); }
}
