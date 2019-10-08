#![warn(rust_2018_idioms)]

#[macro_use]
extern crate log;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use num_traits;
use tokio::sync::{mpsc, Mutex};

use broker::{Broker, ClientId};
use client::FramedPackets;
use core::{MQTTError, MQTTResult, Packet, Publisher};
use std::fmt;
use std::fmt::{Display, Formatter};

mod client;
mod codec;

pub type Tx = mpsc::UnboundedSender<Packet>;
pub type Rx = mpsc::UnboundedReceiver<Packet>;

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

#[derive(Debug)]
pub struct MQTTPublisher {
    client_id: ClientId,
    tx: Tx,
}

impl MQTTPublisher {
    pub fn new(client_id: ClientId, tx: Tx) -> Self {
        MQTTPublisher { tx, client_id }
    }
}

impl Publisher for MQTTPublisher {
    fn publish_msg(&self, packet: Packet) -> MQTTResult<()> {
        self.tx.clone().try_send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }
}

impl Display for MQTTPublisher {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MQTT Publisher: ({})", self.client_id)
    }
}
