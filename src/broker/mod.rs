use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::{fmt, io};

use bytes::Bytes;

use mqtt::*;

mod client;
mod packets;
mod util;

type BrokerError = io::Error;

pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
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

#[derive(Debug)]
pub struct Puback {
    pub packet_id: u16,
}

impl Broker {
    pub fn new() -> Self {
        info!("Broker has started");
        Broker {
            clients: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    pub fn connect(&mut self, connect: Connect, tx: Tx) -> ClientId {
        let client_id = connect.client_id.unwrap();
        info!("Client: {:?} has connected to broker", &client_id);
        self.clients.insert(client_id.to_owned(), tx);
        client_id
    }

    pub fn subscribe(&mut self, client_id: &ClientId, subscribe: Subscribe) -> Result<Vec<u8>, BrokerError> {
        let mut result = vec![];
        for (topic, qos) in subscribe.topics {
            info!("Client: {} has subscribed to topic: {} with qos {}", client_id, topic, qos);

            Broker::validate_subscribe(&topic)?;

            self.subscriptions
                .entry(topic.to_owned())
                .or_insert_with(HashSet::new)
                .insert(client_id.clone());

            result.push(qos);
        }

        Ok(result)
    }

    pub fn publish(&mut self, publish: Publish) -> Result<(), BrokerError> {
        let subscriptions = &mut self.subscriptions;
        let clients = &self.clients;

        Broker::validate_publish(&publish.topic)?;

        for (subscription, client_ips) in subscriptions {
            if Broker::filter_matches_topic(subscription, &publish.topic) {
                client_ips.retain(|client| Broker::publish_msg_or_clear(clients, client, &publish))
            }
        }

        Ok(())
    }

    pub fn acknowledge(&mut self, _packet_id: u16) -> Result<(), io::Error> {
        Ok(())
    }

    fn filter_matches_topic(subscription: &str, topic: &str) -> bool {
        let mut sub_iter = subscription.chars().into_iter();
        let mut topic_iter = topic.chars().into_iter();

        loop {
            let sub_char = sub_iter.next();
            let mut topic_char = topic_iter.next();

            if sub_char == Some('#') {
                //skip multilevel
                return true;
            } else if sub_char == Some('+') {
                //skip one level
                loop {
                    topic_char = topic_iter.next();

                    if topic_char == Some('/') || topic_char == None {
                        sub_iter.next();
                        break;
                    }
                }
            } else {
                //topic and sub are empty
                if topic_char.is_none() {
                    return sub_char.is_none();
                }

                //compare char
                if topic_char != sub_char {
                    return false;
                }
            }
        }
    }

    fn validate_publish(topic: &str) -> Result<(), BrokerError> {
        if topic.chars().all(|c| (char::is_alphanumeric(c) || c == '/') && c != '#' && c != '+') {
            return Ok(());
        }
        return Err(Broker::error("invalid_topic_path"));
    }

    fn validate_subscribe(filter: &str) -> Result<(), BrokerError> {
        let mut peekable = filter.chars().peekable();
        loop {
            match peekable.next() {
                None => return Ok(()),
                Some('#') => {
                    if peekable.peek().is_some() {
                        return Err(Broker::error("invalid_topic_filter"));
                    }
                }
                Some('+') => {
                    if let Some(&next) = peekable.peek() {
                        if next != '/' {
                            return Err(Broker::error("invalid_topic_filter"));
                        }
                    }
                }
                Some('/') => {
                    if let Some(&next) = peekable.peek() {
                        if !(next == '+' || next == '/' || next == '#' || char::is_alphanumeric(next)) {
                            return Err(Broker::error("invalid_topic_filter"));
                        }
                    }
                }
                Some(_) => {
                    if let Some(&next) = peekable.peek() {
                        if !(next == '/' || char::is_alphanumeric(next)) {
                            return Err(Broker::error("invalid_topic_filter"));
                        }
                    }
                }
            }
        }
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

    fn error(msg: &str) -> BrokerError {
        return io::Error::new(io::ErrorKind::Other, msg);
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "Broker(clients#: {}, subscriptions#: {})", self.clients.len(), self.subscriptions.len())
    }
}

#[cfg(test)]
mod tests {
    use futures::sync::mpsc;
    use futures::{Future, Stream};

    use super::*;

    #[test]
    fn test_broker_connect() {
        let mut broker = Broker::new();
        register_client(&mut broker, "client_id");

        assert!(broker.clients.contains_key(&"client_id".to_string()));
    }

    #[test]
    fn test_broker_subscribe_with_incorrect_filter() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);

        for topic in vec![
            "/topic/a+",
            "/topic/a+/",
            "/topic/a+a",
            "/topic/a+a/",
            "/topic/+a",
            "/topic/+a/",
            "/topic/#/a",
            "/topic/a#",
            "/topic/a#/",
            "/topic/#a",
            "/topic/#a/",
        ] {
            let subscribe = Subscribe {
                packet_id: 0,
                topics: vec![(topic.to_string(), 1)].into_iter().collect(),
            };
            assert!(
                broker.subscribe(&client_id.to_string(), subscribe).is_err(),
                "Subscription should fail for filter: {}",
                topic
            );
        }
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
    fn test_broker_subscribe_with_wildcard_topics_single_level_last() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/+"]);

        for topic in vec!["/topic/+"] {
            assert!(broker.subscriptions.contains_key(topic));
            assert!(broker.subscriptions.get(topic).unwrap().contains(client_id));
        }
    }

    #[test]
    fn test_broker_subscribe_with_wildcard_topics_single_level_middle() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/+/topic"]);

        for topic in vec!["/topic/+/topic"] {
            assert!(broker.subscriptions.contains_key(topic));
            assert!(broker.subscriptions.get(topic).unwrap().contains(client_id));
        }
    }

    #[test]
    fn test_broker_subscribe_with_wildcard_topics_multilevel() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/#"]);

        for topic in vec!["/topic/#"] {
            assert!(broker.subscriptions.contains_key(topic));
            assert!(broker.subscriptions.get(topic).unwrap().contains(client_id));
        }
    }

    #[test]
    fn test_broker_publish_to_topic_containing_wildcard() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic"]);

        for topic in vec!["+", "/topic/+a", "/topic/a+b"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_err(), "Publish failed for topic: {}", topic);
        }
    }

    #[test]
    fn test_broker_publish_with_plain_subscription() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic", "/second/topic", "/third/topic"]);

        for topic in vec!["/topic", "/second/topic", "/third/topic"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
        }

        rx.close();
        let packets = rx.collect().wait().unwrap();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x06/topic\0\x01test")));
        assert_eq!(packets[1].payload, Some(Bytes::from("\0\r/second/topic\0\x01test")));
        assert_eq!(packets[2].payload, Some(Bytes::from("\0\x0c/third/topic\0\x01test")));
    }

    #[test]
    fn test_broker_publish_with_wildcard_one_level_subscription() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/+"]);

        for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/level/first", "/different/topic"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
        }

        rx.close();
        let packets = rx.collect().wait().unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
    }

    #[test]
    fn test_broker_publish_with_wildcard_one_level_in_the_middle_subscription() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/+/first"]);

        for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first", "/different/topic"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
        }

        rx.close();
        let packets = rx.collect().wait().unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x15/topic/oneLevel/first\0\x01test")));
    }

    #[test]
    fn test_broker_publish_with_wildcard_multilevel_at_the_end_subscription() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);
        subscribe_client(&mut broker, client_id, &["/topic/#"]);

        for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first", "/different/topic"] {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
        }

        rx.close();
        let packets = rx.collect().wait().unwrap();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
        assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
        assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
    }

    fn subscribe_client(broker: &mut Broker, client_id: &str, topics: &[&str]) {
        let topics = topics.iter().map(|str| (str.to_string(), 1)).collect();
        let subscribe = Subscribe { packet_id: 0, topics };

        assert!(broker.subscribe(&client_id.to_string(), subscribe).is_ok());
    }

    fn register_client(broker: &mut Broker, client_id: &str) -> Rx {
        let connect = Connect {
            version: 3,
            client_id: Some(client_id.to_string()),
            auth: None,
            will: None,
            clean_session: false,
        };
        let (tx, rx) = mpsc::unbounded();

        broker.connect(connect, tx);
        rx
    }
}
