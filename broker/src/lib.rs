#![warn(rust_2018_idioms)]

#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::mpsc;

use bytes::Bytes;

use packets::Packet;

mod broker;

pub type ClientId = String;
pub type PacketId = u128;
pub type Topic = String;

pub type Tx = mpsc::Sender<Packet>;
pub type Rx = mpsc::Receiver<Packet>;

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
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for ApplicationMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }

    fn ne(&self, other: &Self) -> bool {
        self.id != other.id
    }
}
