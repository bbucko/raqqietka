use codec;
use futures::sync::mpsc;
//use std::sync::{Arc, Mutex};
//use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct Broker {}

#[derive(Debug)]
pub struct Client {
    pub client_id: String,
    pub tx: mpsc::UnboundedSender<codec::MQTTPacket>,
}

impl Client {
    pub fn send(&self, msg: codec::MQTTPacket) {
        if let Err(err) = self.tx.unbounded_send(msg) {
            error!("something went wrong: {:?}", err);
        }
    }
}

impl Broker {
    pub fn new() -> Broker { Broker {} }

    pub fn connect(&self, client_id: String) -> (Client, mpsc::UnboundedReceiver<codec::MQTTPacket>) {
        info!("new client: {:?}", client_id);
        let (tx, rx) = mpsc::unbounded::<codec::MQTTPacket>();
        (Client { client_id, tx }, rx)
    }
}
