#![warn(rust_2018_idioms)]

use std::fmt;
use std::fmt::{Display, Formatter};
use std::time::SystemTime;

use futures::prelude::*;
use num_traits;
use tokio::codec::FramedWrite;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tracing::info;

use broker::ClientId;
use core::{MQTTError, MQTTResult, Packet, Publisher};

mod client;
mod codec;

pub type Tx = mpsc::UnboundedSender<Packet>;
pub type Rx = mpsc::UnboundedReceiver<Packet>;

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
        self.tx.clone().try_send(packet).map_err(|e| MQTTError::ServerError(e.to_string()))
    }
}

impl Display for MessageConsumer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Client{{client_id = {}}}", self.client_id)
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
        let mut lines = FramedWrite::new(write, PacketsCodec::default());

        while let Some(msg) = self.rx.next().await {
            match lines.send(msg).await {
                Ok(_) => {}
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
        write!(f, "Client{{client_id = {}}}", self.client_id)
    }
}
