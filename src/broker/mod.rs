use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use bytes::Bytes;

use crate::mqtt::{Packets, Rx, Tx};

mod broker;
mod client;
mod packets;
mod util;

pub type ClientId = String;
pub type PacketId = u128;
pub type Topic = String;

#[derive(Default)]
pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    //    messages: HashSet<Message>,
    //    last_packet: HashMap<ClientId, PacketId>,
}

#[derive(Eq, Debug)]
pub struct Message {
    id: PacketId,
    payload: Bytes,
    topic: Topic,
}

#[derive(Debug)]
pub struct Client {
    pub client_id: ClientId,
    addr: SocketAddr,
    disconnected: bool,
    packets: Packets,
    incoming: Rx,
    broker: Arc<Mutex<Broker>>,
    last_received_packet: SystemTime,
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
pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct Puback {
    pub packet_id: u16,
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
pub struct Unsuback {
    pub packet_id: u16,
}

#[derive(Debug)]
pub struct Suback {
    pub packet_id: u16,
    pub sub_results: Vec<u8>,
}
