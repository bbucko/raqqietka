use futures::sync::mpsc;
use bytes::Bytes;
use client::Client;
use codec::MQTT;
use codec::Packet;
use std::collections::HashMap;

type Tx = mpsc::UnboundedSender<Bytes>;

#[derive(Debug)]
pub struct Broker {
    clients: HashMap<String, Tx>,
}

impl Broker {
    pub fn new() -> Broker {
        Broker { clients: HashMap::new() }
    }

    pub fn register(&mut self, connect: Packet, packets: MQTT) -> Option<Client> {
        if let Some(client) = Client::new(connect, packets) {
            self.register_client(client.client_info.clone(), client.tx.clone());
            return Some(client);
        }
        None
    }

    pub fn register_client(&mut self, client_info: String, tx: Tx) {
        info!("registering {:?}", client_info);
        tx.unbounded_send(Bytes::from(vec![0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000]))
            .unwrap();
        self.clients.insert(client_info, tx);
    }
}
