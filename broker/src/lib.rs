#![warn(rust_2018_idioms)]
#![feature(test)]
#![feature(integer_atomics)]

#[cfg(test)]
extern crate test;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::{channel, Receiver, Sender};

use bytes::Bytes;
use tracing::{error, info};

use core::{MQTTResult, Packet, Publish, Publisher, Subscribe, Unsubscribe};

use crate::validator::{validate_publish, validate_subscribe};

pub type ClientId = String;
pub type PacketId = u64;
pub type Topic = String;

mod validator;

#[derive(Default)]
pub struct Broker {
    clients: HashMap<ClientId, Box<dyn Publisher>>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    message_bus: HashMap<Topic, (Sender<ApplicationMessage>, Receiver<ApplicationMessage>)>,
    packet_counter: HashMap<Topic, std::sync::atomic::AtomicU64>,
    last_packet: HashMap<ClientId, HashMap<Topic, PacketId>>,
}

#[derive(Eq, Debug, Clone)]
struct ApplicationMessage {
    id: PacketId,
    payload: Bytes,
    topic: Topic,
    qos: u8,
}

impl From<ApplicationMessage> for Publish {
    fn from(application_message: ApplicationMessage) -> Self {
        Publish {
            packet_id: application_message.id as u16,
            topic: application_message.topic,
            qos: application_message.qos,
            payload: application_message.payload,
        }
    }
}

impl From<ApplicationMessage> for Packet {
    fn from(application_message: ApplicationMessage) -> Self {
        let publish: Publish = application_message.into();
        publish.into()
    }
}

impl Hash for ApplicationMessage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for ApplicationMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Display for ApplicationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{packet_id = {}, topic = {}, qos = {}}}", self.id, self.topic, self.qos)
    }
}

impl Broker {
    pub fn new() -> Self {
        Broker::default()
    }

    pub fn register(&mut self, client_id: &str, publisher: Box<dyn Publisher>) -> MQTTResult<()> {
        info!("registered new client");

        Broker::send_connack(publisher.as_ref())?;

        self.clients
            .insert(client_id.to_owned(), publisher)
            .and_then(Broker::disconnect_if_session_exists);

        Ok(())
    }

    pub fn subscribe(&mut self, client_id: &str, subscribe: Subscribe) -> MQTTResult<Vec<u8>> {
        let mut result = vec![];
        for (topic, qos) in subscribe.topics {
            validate_subscribe(&topic)?;

            self.message_bus.entry(topic.clone()).or_insert_with(channel);

            self.subscriptions
                .entry(topic.clone())
                .or_insert_with(HashSet::new)
                .insert(client_id.to_owned());

            info!(%topic, "subscribed to");

            result.push(qos);
        }

        Ok(result)
    }

    pub fn unsubscribe(&mut self, client_id: &str, unsubscribe: Unsubscribe) -> MQTTResult<()> {
        for topic in unsubscribe.topics.iter() {
            self.subscriptions.entry(topic.to_owned()).and_modify(|values| {
                values.remove(client_id);
                info!(%topic, "unsubscribed from");
            });
        }

        Ok(())
    }

    pub fn validate(&self, publish: &Publish) -> MQTTResult<()> {
        validate_publish(&publish.topic)
    }

    pub fn publish(&mut self, publish: Publish) -> MQTTResult<()> {
        let topic = publish.topic.clone();

        let subscriptions = &mut self.subscriptions;
        let last_packet_per_topic = &mut self.last_packet;
        let clients = &mut self.clients;

        let packet_id: PacketId = self
            .packet_counter
            .entry(topic.clone())
            .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
            .fetch_add(1, SeqCst);

        let application_message = ApplicationMessage {
            id: packet_id,
            payload: publish.payload,
            topic: topic.clone(),
            qos: publish.qos,
        };

        subscriptions
            .iter()
            .filter(|(subscription, _)| Broker::filter_matches_topic(subscription, &topic))
            .flat_map(|(_, client_ips)| client_ips)
            .map(|client_id| (clients.get(client_id), client_id))
            .filter(|(publisher, _)| publisher.is_some())
            .map(|(publisher, client_id)| (publisher.unwrap(), client_id))
            .for_each(|(publisher, client_id)| {
                let application_message = application_message.clone();
                match publisher.send(application_message.clone().into()) {
                    Ok(()) => {
                        info!(%application_message, %publisher, "published");

                        let application_message_id = application_message.id;
                        let topic = application_message.topic;
                        last_packet_per_topic
                            .entry(client_id.to_string())
                            .or_insert_with(HashMap::new)
                            .insert(topic, application_message_id);
                    }
                    Err(e) => {
                        error!(%e, "occurred while publishing message to {:?}", publisher);
                    }
                };
            });

        Ok(())
    }

    pub fn acknowledge(&mut self, packet_id: u16) -> MQTTResult<()> {
        info!(%packet_id, "acknowledged");

        Ok(())
    }

    fn filter_matches_topic(subscription: &str, topic: &str) -> bool {
        let mut sub_iter = subscription.chars();
        let mut topic_iter = topic.chars();

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

    pub fn disconnect(&mut self, client_id: ClientId) {
        info!("disconnected");

        self.clients.remove(&client_id);
        self.subscriptions.iter_mut().for_each(|(_, clients)| {
            clients.remove(&client_id);
        });
    }

    fn send_connack(publisher: &dyn Publisher) -> MQTTResult<()> {
        let connack = core::ConnAck::default();
        publisher.send(connack.into())
    }

    fn disconnect_if_session_exists(existing_client: Box<dyn Publisher>) -> Option<()> {
        let disconnect = core::Disconnect::default();
        let result = existing_client.send(disconnect.into());
        match result {
            Ok(_) => info!("session takeover"),
            Err(error) => error!(%error, "error while disconnecting existing client"),
        }
        None
    }
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Broker {{clients = {}, subscriptions = {}}}", self.clients.len(), self.subscriptions.len())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;
    use tokio::sync::mpsc;

    use core::{Connect, PacketType, Publish, Subscribe};
    use mqtt::{MessageConsumer, Rx, Tx};

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
        subscribe_client(&mut broker, client_id, &simple_topics());

        for topic in simple_topics() {
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
    fn test_broker_publish_without_subscription() {
        let mut broker = Broker::new();
        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);

        for topic in simple_topics() {
            let publish = Publish {
                packet_id: 1,
                topic: topic.to_owned(),
                qos: 0,
                payload: Bytes::from("test"),
            };
            assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
        }

        rx.close();
        assert!(block_on(rx.recv()).is_none());
    }

    #[bench]
    fn test_publish_through_direct(b: &mut test::Bencher) {
        let (tx, _) = mpsc::unbounded_channel();
        let mut map: HashMap<String, Tx> = HashMap::new();
        map.insert("abc".to_string(), tx);

        b.iter(|| map.get("abc").unwrap().clone().send(create_packet()));
    }

    #[bench]
    fn test_publish_through_trait(b: &mut test::Bencher) {
        let (tx, _) = mpsc::unbounded_channel();
        let mut map: HashMap<String, Box<dyn Publisher>> = HashMap::new();
        map.insert("abc".to_string(), Box::new(MessageConsumer::new("abc".to_string(), tx)));

        b.iter(|| map.get_mut("abc").unwrap().send(create_packet()));
    }

    fn simple_topics() -> Vec<&'static str> {
        vec!["/topic", "/second/topic", "/third/topic"]
    }

    fn subscribe_client(broker: &mut Broker, client_id: &str, topics: &[&str]) {
        let topics = topics.iter().map(|str| (str.to_string(), 1)).collect();
        let subscribe = Subscribe { packet_id: 0, topics };

        assert!(broker.subscribe(&client_id.to_string(), subscribe).is_ok());
    }

    fn register_client(broker: &mut Broker, client_id: &str) -> Rx {
        let connect = Connect {
            version: 3,
            client_id: Some(client_id.clone().to_string()),
            auth: None,
            will: None,
            clean_session: false,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();

        let _ = broker.register(&connect.client_id.unwrap(), Box::new(MessageConsumer::new(client_id.clone().to_string(), tx)));
        assert_eq!(block_on(rx.recv()).unwrap().packet_type, core::PacketType::CONNACK);

        rx
    }

    fn create_packet() -> Packet {
        Packet {
            packet_type: PacketType::CONNECT,
            flags: 0,
            payload: Option::None,
        }
    }
}
