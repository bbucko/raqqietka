mod handlers;

use self::handlers::{MQTTPacket, Type};
use bytes::Bytes;
use futures::sync::mpsc;
use codec::{MQTT as Codec, Packet};
use tokio::io;
use tokio::prelude::*;
use std::sync::Arc;
use broker::Broker;

type Tx = mpsc::UnboundedSender<Bytes>;
type Rx = mpsc::UnboundedReceiver<Bytes>;

static LINES_PER_TICK: usize = 10;

#[derive(Debug)]
pub struct Client {
    pub client_id: String,
    packets: Codec,
    rx: Rx,
    broker: Arc<Broker>,
}

impl Client {
    pub fn id(&self) -> String {
        self.client_id.clone()
    }

    pub fn new(packet: Packet, packets: Codec, broker: Arc<Broker>) -> Option<(Self, Tx)> {
        match handlers::connect(&packet.payload) {
            Ok(Some((client_id, _username, _password, _will))) => Some(Client::create(client_id, packets, broker)),
            _ => None,
        }
    }

    fn response(&self, msg: Vec<u8>) {
        self.broker.publish_message(&self.client_id, msg);
    }

    fn create(client_info: String, packets: Codec, broker: Arc<Broker>) -> (Self, Tx) {
        let (tx, rx) = mpsc::unbounded();

        (
            Self {
                client_id: client_info,
                packets: packets,
                rx: rx,
                broker: broker,
            },
            tx,
        )
    }
}

impl Future for Client {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        debug!("flush outbound queue: {:?}", self.rx);
        for i in 0..LINES_PER_TICK {
            match self.rx.poll().unwrap() {
                Async::Ready(Some(v)) => {
                    debug!("sending packets: {:?}", v);
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
                info!("rq: {:?}", rq);

                if let Some(rq) = rq {
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
    fn drop(&mut self) {
        info!("Disconecting: {:?}", self);
    }
}
