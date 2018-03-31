use futures::sync::mpsc;
use bytes::Bytes;
use client::Client;
use codec::MQTT;
use codec::Packet;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type Tx = mpsc::UnboundedSender<Bytes>;

#[derive(Debug)]
pub struct Broker {
    clients: RwLock<HashMap<String, Tx>>,
}

impl Broker {
    pub fn new() -> Broker {
        Broker {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn rgs(connect: Packet, packets: MQTT, broker: Arc<Broker>) -> Option<Client> {
        if let Some((client, tx)) = Client::new(connect, packets, broker.clone()) {
            broker.register_client(client.id(), tx);
            return Some(client);
        }
        None
    }

    pub fn publish_message(&self, client_id: &String, msg: Vec<u8>) {
        match self.clients.read().unwrap().get(client_id) {
            Some(tx) => tx.unbounded_send(Bytes::from(msg)).unwrap(),
            None => info!("unknown client"),
        }
    }

    pub fn register_client(&self, client_info: String, tx: Tx) {
        info!("registering {:?}", client_info);
        self.clients.write().unwrap().insert(client_info.clone(), tx);
        self.publish_message(&client_info, vec![0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000]);
    }
}
