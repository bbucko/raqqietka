#![warn(rust_2018_idioms)]

use std::fmt;
use std::fmt::{Display, Formatter};
use std::time::SystemTime;

use futures::prelude::*;
use num_traits;
use tokio::io::AsyncWrite;
use tokio_util::codec::FramedWrite;
use tracing::info;

use core::*;

mod client;
mod codec;

pub type Tx = tokio::sync::mpsc::UnboundedSender<Packet>;
pub type Rx = tokio::sync::mpsc::UnboundedReceiver<Packet>;

#[derive(Debug, Default)]
pub struct PacketsCodec {}

#[derive(Debug)]
pub struct Client {
    id: ClientId,
    disconnected: bool,
    last_received_packet: SystemTime,
}

#[derive(Debug, Clone)]
pub struct MessageConsumer {
    client_id: ClientId,
    tx: Tx,
}

impl MessageConsumer {
    pub fn new(client_id: ClientId, tx: Tx) -> Self {
        MessageConsumer { tx, client_id }
    }
}

impl Publisher for MessageConsumer {
    fn send(&self, packet: Packet) -> MQTTResult<()> {
        self.tx.clone().send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}

#[derive(Debug)]
pub struct MessageProducer {
    client_id: ClientId,
    rx: Rx,
}

impl MessageProducer {
    pub fn new(client_id: ClientId, rx: Rx) -> Self {
        MessageProducer { rx, client_id }
    }
}

impl MessageProducer {
    pub async fn forward_to<W>(mut self, write: W)
    where
        W: AsyncWrite + Unpin,
        FramedWrite<W, PacketsCodec>: Sink<Packet>,
        <FramedWrite<W, PacketsCodec> as Sink<Packet>>::Error: fmt::Display,
    {
        let mut lines = FramedWrite::new(write, PacketsCodec::new());

        while let Some(msg) = self.rx.next().await {
            let packet_type = msg.packet_type.clone();
            match lines.send(msg).await {
                Ok(_) => {
                    if packet_type == PacketType::DISCONNECT {
                        return;
                    }
                }
                Err(error) => {
                    info!(%error, "error sending to client");
                    return;
                }
            }
        }

        // The client has disconnected, we can stop forwarding.
        info!("client disconnected");
    }
}

impl Display for MessageProducer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{client_id = {}}}", self.client_id)
    }
}
