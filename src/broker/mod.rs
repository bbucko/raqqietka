use bytes::Bytes;
use client::Client;
use codec::MQTT;
use codec::Packet;
use futures::sync::mpsc;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

type Tx = mpsc::UnboundedSender<Bytes>;

#[derive(Debug)]
pub struct Broker {
    clients: RwLock<HashMap<SocketAddr, Tx>>,
}

impl Broker {
    pub fn new() -> Broker {
        Broker {
            clients: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(connect: &Packet, packets: MQTT, broker: &Arc<Broker>, addr: SocketAddr) -> Option<Client> {
        if let Some((client, tx)) = Client::new(connect, packets, broker.clone(), addr) {
            broker.register_client(&client, tx);
            return Some(client);
        }
        None
    }

    pub fn publish_message(&self, client: &Client, msg: Vec<u8>) {
        let client_id = client.id();

        info!("publishing message: {:?} to client: {:?}", msg, client_id);
        match self.clients.read().unwrap().get(client_id) {
            Some(tx) => tx.unbounded_send(Bytes::from(msg)).unwrap(),
            None => panic!("unknown client"),
        }
    }

    pub fn unregister(client: &Client, broker: &Arc<Broker>) {
        let client_id = client.id();

        info!("unregistering {:?}", client_id);
        let mut clients = broker.clients.write().unwrap();
        clients.remove(client_id);
        info!("unregistered {:?}; clients registered: {}", client_id, clients.len());
    }

    fn register_client(&self, client: &Client, tx: Tx) {
        {
            let client_id = client.id();
            let mut clients = self.clients.write().unwrap();

            info!("registering {:?}; clients registered: {}", client_id, clients.len());
            clients.insert(client_id.clone(), tx);
        }
        self.publish_message(client, vec![0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000]);
    }
}
