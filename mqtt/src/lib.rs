#![warn(rust_2018_idioms)]

#[macro_use]
extern crate enum_primitive_derive;

use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::buf::BufMut;
use bytes::{Bytes, BytesMut};
use futures::prelude::*;
use futures::SinkExt;
use num_traits;
use num_traits::cast::ToPrimitive;
use tokio::io::AsyncWrite;
use tokio::sync;
use tokio_util::codec::FramedWrite;
use tracing::{debug, error, info};

use broker::Broker;
use core::*;

mod client;
mod codec;
mod packet;

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
    fn send(&self, packet: Message) -> MQTTResult<()> {
        self.tx.clone().send(packet.into()).map_err(|e| MQTTError::ServerError(e.to_string()))
    }

    fn ack(&self, ack_type: Ack) -> Result<(), MQTTError> {
        let packet = match ack_type {
            Ack::Connect => ConnAck::default().into(),
            Ack::Publish(packet_id) => PubAck { packet_id }.into(),
            Ack::Subscribe(packet_id, sub_results) => SubAck { packet_id, sub_results }.into(),
            Ack::Ping => PingResp::default().into(),
        };
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

#[derive(Debug, Clone, PartialEq)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

#[derive(Debug, Primitive, PartialEq, Clone)]
pub enum PacketType {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    //    PUBREC = 5,
    //    PUBREL = 6,
    //    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRES = 13,
    DISCONNECT = 14,
}

#[derive(Debug)]
pub struct Connect {
    pub version: u8,
    pub client_id: Option<ClientId>,
    pub auth: Option<ConnectAuth>,
    pub will: Option<Will>,
    pub clean_session: bool,
}

#[derive(Debug)]
pub struct ConnectAuth {
    pub username: String,
    pub password: Option<Bytes>,
}

#[derive(Debug)]
pub struct Will {
    pub qos: Qos,
    pub retain: bool,
    pub topic: Topic,
    pub message: Bytes,
}

#[derive(Debug, Default)]
pub struct ConnAck {}

#[derive(Debug)]
pub struct Subscribe {
    pub packet_id: PacketId,
    pub topics: Vec<(Topic, Qos)>,
}

#[derive(Debug)]
pub struct SubAck {
    pub packet_id: PacketId,
    pub sub_results: Vec<Qos>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    pub packet_id: PacketId,
    pub topics: Vec<Topic>,
}

#[derive(Debug)]
pub struct UnsubAck {
    pub packet_id: PacketId,
}

#[derive(Debug)]
pub struct Publish {
    pub packet_id: PacketId,
    pub topic: Topic,
    pub qos: Qos,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct PubAck {
    pub packet_id: PacketId,
}

#[derive(Debug, Default)]
pub struct PingResp {}

#[derive(Debug, Default)]
pub struct Disconnect {}

impl From<Will> for Message {
    fn from(will: Will) -> Self {
        Message {
            id: 1,
            payload: will.message,
            topic: will.topic,
            qos: will.qos,
        }
    }
}
