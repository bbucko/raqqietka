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
pub type ControllerTx = sync::mpsc::UnboundedSender<Command>;

pub enum Command {
    PACKET(Packet),
    DISCONNECT,
}

#[derive(Debug, Default)]
pub struct PacketsCodec {}

#[derive(Debug)]
pub struct Client {
    broker: MqttBroker,
    id: ClientId,
    last_received_packet: SystemTime,
    connected_on: SystemTime,
    controller: ControllerTx,
}

#[derive(Debug, Clone)]
pub struct MessageConsumer {
    client_id: ClientId,
    tx: Tx,
    connected_on: SystemTime,
}

#[derive(Debug)]
pub struct MessageProducer {
    client_id: ClientId,
    rx: Rx,
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

impl PubAck {
    pub fn new(packet_id: PacketId) -> Self {
        PubAck { packet_id }
    }
}

impl SubAck {
    pub fn new(packet_id: PacketId, sub_results: Vec<Qos>) -> Self {
        SubAck { packet_id, sub_results }
    }
}

impl UnsubAck {
    pub fn new(packet_id: PacketId) -> Self {
        UnsubAck { packet_id }
    }
}

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
