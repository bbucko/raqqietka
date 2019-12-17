#![warn(rust_2018_idioms)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::{channel, Receiver, Sender};

use bytes::Bytes;
use tracing::{debug, error, info};

use core::*;

use crate::validator::{validate_publish, validate_subscribe};

mod validator;

pub struct Broker<T>
where
    T: Publisher + Clone,
{
    clients: HashMap<ClientId, T>,
    subscriptions: HashMap<Topic, HashSet<ClientId>>,
    will_messages: HashMap<ClientId, ApplicationMessage>,
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

impl From<Will> for ApplicationMessage {
    fn from(will: Will) -> Self {
        ApplicationMessage {
            id: 1,
            payload: will.message,
            topic: will.topic,
            qos: will.qos,
        }
    }
}

impl<T: Publisher + Clone> Broker<T> {
    pub fn new() -> Self {
        Broker {
            clients: Default::default(),
            subscriptions: Default::default(),
            will_messages: Default::default(),
            message_bus: Default::default(),
            packet_counter: Default::default(),
            last_packet: Default::default(),
        }
    }

    pub fn register(&mut self, client_id: &str, publisher: T, will_message: Option<Will>) -> MQTTResult<()> {
        info!("registered new client");

        let client_id = client_id.to_owned();

        if let Some(_) = self.clients.insert(client_id.clone(), publisher.clone()) {
            self.cleanup(client_id.clone());
        };

        let existing_will_message = will_message
            .map(|will_message| will_message.into())
            .map(|application_message: ApplicationMessage| self.will_messages.insert(client_id.clone(), application_message))
            .flatten();

        assert!(existing_will_message.is_none());

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
        let id = self
            .packet_counter
            .entry(publish.topic.clone())
            .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
            .fetch_add(1, SeqCst);

        let payload = publish.payload;
        let topic = publish.topic;
        let qos = publish.qos;

        self.publish_message(ApplicationMessage { id, payload, topic, qos });

        Ok(())
    }

    pub fn acknowledge(&mut self, packet_id: u16) -> MQTTResult<()> {
        info!(%packet_id, "acknowledged");

        Ok(())
    }

    pub fn disconnect(&mut self, client_id: ClientId) {
        debug!("removing LWT");

        self.will_messages.remove(&client_id);
    }

    pub fn cleanup(&mut self, client_id: ClientId) {
        info!("client disconnected");

        self.clients.remove(&client_id);

        self.subscriptions.iter_mut().for_each(|(_, clients)| {
            clients.remove(&client_id);
        });

        self.will_messages.remove(&client_id).map(|last_will| {
            info!(topic = %last_will.topic, "publishing LWT");
            self.publish_message(last_will)
        });
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

    fn publish_message(&mut self, application_message: ApplicationMessage) {
        let last_packet_per_topic = &mut self.last_packet;
        let clients = &mut self.clients;
        let subscriptions = &mut self.subscriptions;

        subscriptions
            .iter()
            .filter(|(subscription, _)| Broker::<T>::filter_matches_topic(subscription, &application_message.topic))
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
    }
}

impl<T: Publisher + Clone> std::fmt::Debug for Broker<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Broker {{clients = {}, subscriptions = {}}}", self.clients.len(), self.subscriptions.len())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use core::*;
    use mqtt::{MessageConsumer, Rx};

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

            let result = broker.subscribe(&client_id.to_owned(), subscribe);
            assert!(result.is_err(), "Subscription should fail for filter: {}", topic);
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

    fn simple_topics() -> Vec<&'static str> {
        vec!["/topic", "/second/topic", "/third/topic"]
    }

    fn subscribe_client(broker: &mut Broker<MessageConsumer>, client_id: &str, topics: &[&str]) {
        let topics = topics.iter().map(|str| (str.to_string(), 1)).collect();
        let subscribe = Subscribe { packet_id: 0, topics };

        assert!(broker.subscribe(&client_id.to_string(), subscribe).is_ok());
    }

    fn register_client(broker: &mut Broker<MessageConsumer>, client_id: &str) -> Rx {
        let connect = Connect {
            version: 3,
            client_id: Some(client_id.clone().to_string()),
            auth: None,
            will: None,
            clean_session: false,
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let message_consumer = MessageConsumer::new(client_id.clone().to_string(), tx);
        let result = broker.register(&connect.client_id.unwrap(), message_consumer, None);
        assert!(result.is_ok());

        rx
    }
}
