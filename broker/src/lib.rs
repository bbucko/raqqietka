#![warn(rust_2018_idioms)]

#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tokio::sync::mpsc;

mod broker;
mod mqtt_error;
mod packet;
pub mod util;

pub type ClientId = String;
pub type PacketId = u128;
pub type Topic = String;

pub type Tx = mpsc::UnboundedSender<Packet>;
pub type Rx = mpsc::UnboundedReceiver<Packet>;

#[derive(Debug)]
pub enum MQTTError {
    ClientError,
    ServerError(String),
    OtherError(String),
}

//Should be in mqtt
#[derive(Debug, Clone)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

//Should be in mqtt
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
    username: String,
    password: Option<Bytes>,
}

#[derive(Debug)]
pub struct Will {
    qos: u8,
    retain: bool,
    topic: String,
    message: Bytes,
}

#[derive(Debug)]
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: HashSet<(Topic, u8)>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub topics: HashSet<Topic>,
}

#[derive(Debug)]
pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Default)]
pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    messages: HashSet<ApplicationMessage>,
    last_packet: HashMap<ClientId, HashMap<Topic, PacketId>>,
}

#[derive(Eq, Debug)]
pub struct ApplicationMessage {
    id: PacketId,
    payload: Bytes,
    topic: Topic,
}

impl Hash for ApplicationMessage {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state); }
}

impl PartialEq for ApplicationMessage {
    fn eq(&self, other: &Self) -> bool { self.id == other.id }

    fn ne(&self, other: &Self) -> bool { self.id != other.id }
}
