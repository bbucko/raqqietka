#![warn(rust_2018_idioms)]

#[macro_use]
extern crate log;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use num_traits;
use tokio::sync::Mutex;

use broker::{Broker, ClientId, Rx};
use client::FramedPackets;
use packets::Packet;

mod client;
mod codec;

#[derive(Debug, Default)]
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
