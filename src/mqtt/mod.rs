use bytes::Bytes;
use bytes::BytesMut;
use futures::sync::mpsc;
use tokio::net::TcpStream;

mod packet;
mod packets;
mod util;

pub type ClientId = String;
pub type Topic = String;

pub type Tx = mpsc::UnboundedSender<Packet>;
pub type Rx = mpsc::UnboundedReceiver<Packet>;

#[derive(Debug)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

#[derive(Debug)]
pub struct Packets {
    pub socket: TcpStream,
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
