use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::result;

use bytes::Bytes;

mod mqtt_error;
pub mod util;

pub type ClientId = String;
pub type MessageId = u64;
pub type PacketId = u16;
pub type Topic = String;
pub type Qos = u8;

pub type MQTTResult<T> = result::Result<T, MQTTError>;

pub trait Publisher: Send + Debug + Display {
    fn send(&self, packet: Message) -> MQTTResult<()>;

    fn ack(&self, ack: Ack) -> MQTTResult<()>;

    fn disconnect(&self);
}

#[derive(Debug)]
pub enum Ack {
    Connect,
    Publish(PacketId),
    Subscribe(PacketId, Vec<Qos>),
    Unsubscribe(PacketId),
    Ping,
}

#[derive(Eq, Debug, Clone)]
pub struct Message {
    pub id: MessageId,
    pub payload: Bytes,
    pub topic: Topic,
    pub qos: Qos,
}

#[derive(Debug, PartialEq)]
pub enum MQTTError {
    ClientError(String),
    ServerError(String),
    ClosedClient(String),
    OtherError(String),
}

impl Message {
    pub fn new(id: MessageId, topic: Topic, qos: Qos, payload: Bytes) -> Self {
        Message { id, topic, qos, payload }
    }
}

impl Hash for Message {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{packet_id = {}, topic = {}, qos = {}}}", self.id, self.topic, self.qos)
    }
}
