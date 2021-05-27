use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

use broker::Broker;
use bytes::Bytes;
use core::*;
use futures::prelude::*;
use futures::SinkExt;
use tokio::io::AsyncWrite;
use tokio::sync;
use tokio_util::codec::FramedWrite;
use tracing::*;

mod client;
mod codec;

pub type MqttBroker = Arc<sync::Mutex<Broker<MessageConsumer>>>;

pub type Tx = sync::mpsc::UnboundedSender<mqttrs::Packet>;
pub type Rx = sync::mpsc::UnboundedReceiver<mqttrs::Packet>;
pub type ControllerTx = sync::mpsc::UnboundedSender<Command>;

pub enum Command {
    PACKET(mqttrs::Packet),
    DISCONNECT,
}

#[derive(Default)]
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
