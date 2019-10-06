#![warn(rust_2018_idioms)]

#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use tokio::sync::mpsc;

mod broker;
mod mqtt_error;
mod packet;
pub mod util;

pub type ClientId = String;
pub type PacketId = u128;
pub type Topic = String;

pub type Tx = mpsc::UnboundedSender<Packet>;
pub type Rx = mpsc::UnboundedReceiver<Packet>;

#[derive(Debug)]
pub enum MQTTError {
    ClientError,
    ServerError(String),
    OtherError(String),
}

//Should be in mqtt
#[derive(Debug, Clone)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

//Should be in mqtt
#[derive(Debug, Primitive, PartialEq, Clone)]
pub enum PacketType {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    //    PUBREC = 5,
    //    PUBREL = 6,
    //    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRES = 13,
    DISCONNECT = 14,
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
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: HashSet<(Topic, u8)>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub topics: HashSet<Topic>,
}

#[derive(Debug)]
pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Default)]
pub struct Broker {
    clients: HashMap<ClientId, Tx>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    messages: HashSet<ApplicationMessage>,
    last_packet: HashMap<ClientId, HashMap<Topic, PacketId>>,
}

#[derive(Eq, Debug)]
pub struct ApplicationMessage {
    id: PacketId,
    payload: Bytes,
    topic: Topic,
}

impl Hash for ApplicationMessage {
    fn hash<H: Hasher>(&self, state: &mut H) { self.id.hash(state); }
}

impl PartialEq for ApplicationMessage {
    fn eq(&self, other: &Self) -> bool { self.id == other.id }

    fn ne(&self, other: &Self) -> bool { self.id != other.id }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

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
    fn test_broker_validate_topic_containing_wildcard() {
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
            assert!(broker.validate(&publish).is_err(), "Publish failed for topic: {}", topic);
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
        //        let packets = rx.collect().wait().unwrap();
        //        assert_eq!(packets.len(), 3);
        //        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x06/topic\0\x01test")));
        //        assert_eq!(packets[1].payload, Some(Bytes::from("\0\r/second/topic\0\x01test")));
        //        assert_eq!(packets[2].payload, Some(Bytes::from("\0\x0c/third/topic\0\x01test")));
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
        //        let packets = rx.collect().wait().unwrap();
        //        assert_eq!(packets.len(), 1);
        //        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
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
        //        let packets = rx.collect().wait().unwrap();
        //        assert_eq!(packets.len(), 1);
        //        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x15/topic/oneLevel/first\0\x01test")));
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
        //        let packets = rx.collect().wait().unwrap();
        //        assert_eq!(packets.len(), 3);
        //        assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
        //        assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
        //        assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
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
        let (tx, rx) = mpsc::unbounded_channel();

        let _ = broker.register(&connect.client_id.unwrap(), tx);

        rx
    }
}
