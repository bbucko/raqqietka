use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Error, Formatter};
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use regex_cache::RegexCache;

use mqtt::*;

mod client;
mod packets;
mod util;

pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    //change to LRU cache
    regex_cache: RegexCache,
}

#[derive(Debug)]
pub struct Client {
    pub client_id: ClientId,
    addr: SocketAddr,
    disconnected: bool,
    packets: Packets,
    incoming: Rx,
    broker: Arc<Mutex<Broker>>,
}

#[derive(Debug)]
pub struct Connect {
    pub version: u8,
    pub client_id: Option<ClientId>,
    pub auth: Option<ConnectAuth>,
    pub will: Option<Will>,
    pub clean_session: bool,
}

#[derive(Debug)]
pub struct ConnectAuth {
    username: String,
    password: Option<Bytes>,
}

#[derive(Debug)]
pub struct Will {
    qos: u8,
    retain: bool,
    topic: String,
    message: Bytes,
}

#[derive(Debug)]
pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: HashSet<(Topic, u8)>,
}

impl Broker {
    pub fn new() -> Self {
        info!("Broker has started");
        Broker {
            clients: HashMap::new(),
            subscriptions: HashMap::new(),
            regex_cache: RegexCache::new(1024),
        }
    }

    pub fn connect(&mut self, connect: Connect, tx: Tx) -> ClientId {
        let client_id = connect.client_id.unwrap();
        info!("Client: {:?} has connected to broker", &client_id);
        self.clients.insert(client_id.to_owned(), tx);
        client_id
    }

    pub fn subscribe(&mut self, client_id: &ClientId, subscribe: Subscribe) -> Result<Vec<u8>, io::Error> {
        let mut result = vec![];
        for (topic, qos) in subscribe.topics {
            info!("Client: {} has subscribed to topic: {} with qos {}", client_id, topic, qos);
            self.subscriptions
                .entry(topic.to_owned())
                .or_insert_with(HashSet::new)
                .insert(client_id.clone());

            result.push(qos);
        }

        Ok(result)
    }

    pub fn publish(&mut self, publish: Publish) -> Result<(), io::Error> {
        let subscriptions = &mut self.subscriptions;
        let clients = &self.clients;

        for (subscription, client_ips) in subscriptions {
            if subscription.contains("*") || subscription.contains("?") {
                //TODO: replace wildcards with regexp
                let wildcard_topic = self.regex_cache.compile(subscription).unwrap();
                if wildcard_topic.is_match(&publish.topic) {
                    client_ips.retain(|client| Broker::publish_msg_or_clear(clients, client, &publish))
                }
            } else if subscription.eq(&publish.topic) {
                client_ips.retain(|client| Broker::publish_msg_or_clear(clients, client, &publish))
            }
        }

        Ok(())
    }

    pub fn disconnect(&mut self, client_id: ClientId) {
        info!("Client: {:?} has disconnected from broker", client_id);
        self.clients.remove(&client_id);
    }

    fn publish_msg_or_clear(clients: &HashMap<ClientId, Tx>, client: &String, publish: &Publish) -> bool {
        if let Some(tx) = clients.get(client) {
            let publish_packet = Packet::publish(publish.packet_id, publish.topic.to_owned(), publish.payload.clone(), publish.qos);
            info!("Sending payload: {} to client: {}", publish_packet, client);
            let _ = tx.unbounded_send(publish_packet);
            true
        } else {
            info!("Removing disconnected client: {}", client);
            false
        }
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Broker(clients#: {}, subscriptions#: {})", self.clients.len(), self.subscriptions.len())
    }
}

#[cfg(test)]
mod tests {
    use futures::sync::mpsc;

    use super::*;

    #[test]
    fn test_broker_connect() {
        let mut broker = Broker::new();
        register_client(&mut broker, "client_id");

        assert!(broker.clients.contains_key(&"client_id".to_string()));
    }

    #[test]
    fn test_broker_subscribe_with_plain_topics() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic", "/second/topic", "/third/topic"]);

        for topic in vec!["/topic", "/second/topic", "/third/topic"] {
            assert!(broker.subscriptions.contains_key(topic));
            assert!(broker.subscriptions.get(topic).unwrap().contains(client_id));
        }
    }

    #[test]
    fn test_broker_subscribe_with_wildcard_topics() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/*", "/topic/?", "/topic/?/first"]);

        for topic in vec!["/topic/*", "/topic/?", "/topic/?/first"] {
            assert!(broker.subscriptions.contains_key(topic));
            assert!(broker.subscriptions.get(topic).unwrap().contains(client_id));
        }
    }

    #[test]
    fn test_broker_publish_with_plain() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic", "/second/topic", "/third/topic"]);

        for topic in vec!["/topic", "/second/topic", "/third/topic"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok());
        }
    }

    #[test]
    fn test_broker_publish_with_wildcard() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/*", "/topic/?", "/topic/?/first"]);

        for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok());
        }
    }

    fn subscribe_client(broker: &mut Broker, client_id: &str, topics: &[&str]) {
        let topics = topics.iter().map(|str| (str.to_string(), 1)).collect();
        let subscribe = Subscribe { packet_id: 0, topics };

        assert!(broker.subscribe(&client_id.to_string(), subscribe).is_ok());
    }

    fn register_client(broker: &mut Broker, client_id: &str) {
        let connect = Connect {
            version: 3,
            client_id: Some(client_id.to_string()),
            auth: None,
            will: None,
            clean_session: false,
        };
        let (tx, _) = mpsc::unbounded();

        broker.connect(connect, tx);
    }
}
