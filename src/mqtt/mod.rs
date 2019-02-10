use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use bytes::BytesMut;
use futures::sync::mpsc;
use tokio::net::TcpStream;

mod broker;
mod client;
mod packet;
mod packets;
mod util;

type ClientId = String;
type Topic = String;
type Tx = mpsc::UnboundedSender<Packet>;
type Rx = mpsc::UnboundedReceiver<Packet>;

#[allow(dead_code)]
#[derive(Debug)]
pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
}

#[derive(Debug)]
pub struct Client {
    client_id: ClientId,
    addr: SocketAddr,
    disconnected: bool,
    packets: Packets,
    incoming: Rx,
    broker: Arc<Mutex<Broker>>,
}

#[derive(Debug)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

#[derive(Debug)]
pub struct Packets {
    socket: TcpStream,
    rd: BytesMut,
    wr: BytesMut,
}

#[derive(Debug, Primitive, PartialEq)]
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
