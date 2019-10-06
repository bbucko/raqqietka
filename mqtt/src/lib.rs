#![warn(rust_2018_idioms)]

#[macro_use]
extern crate log;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use num_traits;
use tokio::sync::Mutex;

use broker::{util, Broker, ClientId, Packet, Rx};
use client::FramedPackets;

mod client;
mod codec;
mod packets;

#[derive(Debug)]
pub struct PacketsCodec {}

#[derive(Debug)]
pub enum Message {
    Broadcast(Packet),
    Received(Packet),
}

#[derive(Debug)]
pub struct Client {
    pub client_id: ClientId,
    addr: SocketAddr,
    disconnected: bool,
    packets: FramedPackets,
    incoming: Rx,
    broker: Arc<Mutex<Broker>>,
    last_received_packet: SystemTime,
}

#[derive(Debug)]
pub struct ConnAck {}

#[derive(Debug)]
pub struct PubAck {
    pub packet_id: u16,
}

#[derive(Debug)]
pub struct SubAck {
    pub packet_id: u16,
    pub sub_results: Vec<u8>,
}

#[derive(Debug)]
pub struct UnsubAck {
    pub packet_id: u16,
}

#[derive(Debug)]
pub struct PingResp {}
