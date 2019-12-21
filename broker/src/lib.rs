#![warn(rust_2018_idioms)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
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
    will_messages: HashMap<ClientId, Message>,
    message_bus: HashMap<Topic, (Sender<Message>, Receiver<Message>)>,
    packet_counter: HashMap<Topic, std::sync::atomic::AtomicU64>,
    last_packet: HashMap<ClientId, HashMap<Topic, GlobalPacketId>>,
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

    pub fn register(&mut self, client_id: &str, publisher: T, will_message: Option<Message>) -> MQTTResult<()> {
        info!("registered new client");

        let client_id = client_id.to_owned();

        if let Some(_) = self.clients.insert(client_id.clone(), publisher.clone()) {
            self.force_disconnect(client_id.clone());
        };

        let existing_will_message = will_message
            .map(|application_message: Message| self.will_messages.insert(client_id.clone(), application_message))
            .flatten();

        assert!(existing_will_message.is_none());

        Ok(())
    }

    pub fn subscribe(&mut self, client_id: &str, subscribe: Vec<(Topic, Qos)>) -> MQTTResult<Vec<Qos>> {
        let mut result = vec![];
        for (topic, qos) in subscribe {
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

    pub fn unsubscribe(&mut self, client_id: &str, topics: Vec<Topic>) -> MQTTResult<()> {
        for topic in topics.iter() {
            self.subscriptions.entry(topic.to_owned()).and_modify(|values| {
                values.remove(client_id);
                info!(%topic, "unsubscribed from");
            });
        }

        Ok(())
    }

    pub fn validate(&self, topic: &Topic) -> MQTTResult<()> {
        validate_publish(topic)
    }

    pub fn publish(&mut self, topic: Topic, qos: Qos, payload: Bytes) -> MQTTResult<()> {
        let id = self
            .packet_counter
            .entry(topic.clone())
            .or_insert_with(|| std::sync::atomic::AtomicU64::new(0))
            .fetch_add(1, SeqCst);

        let message = Message { id, topic, qos, payload };
        self.publish_message(message);

        Ok(())
    }

    pub fn acknowledge(&mut self, packet_id: u16) -> MQTTResult<()> {
        info!(%packet_id, "acknowledged");

        Ok(())
    }

    pub fn disconnect_cleanly(&mut self, client_id: ClientId) {
        debug!("removing LWT");

        self.will_messages.remove(&client_id);
    }
    pub fn force_disconnect(&mut self, client_id: ClientId) {
        debug!("forcing disconnect");

        self.will_messages.remove(&client_id).map(|last_will| {
            info!(topic = %last_will.topic, "publishing LWT");
            self.publish_message(last_will)
        });
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

    fn publish_message(&mut self, application_message: Message) {
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
    use tokio::sync::mpsc;

    use mqtt::{Connect, MessageConsumer, Rx};

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
            let subscribe = vec![(topic.to_string(), 1)].into_iter().collect();
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
            assert!(broker.validate(&topic.to_string()).is_err(), "Publish failed for topic: {}", topic);
        }
    }

    #[test]
    fn test_broker_publish_without_subscription() {
        let mut broker = Broker::new();

        let client_id = "client_id";
        let mut rx = register_client(&mut broker, client_id);

        for topic in simple_topics() {
            assert!(
                broker.publish(topic.to_string(), 1, Bytes::from("test")).is_ok(),
                "Publish failed for topic: {}",
                topic
            );
        }

        rx.close();
        assert!(block_on(rx.recv()).is_none());
    }

    fn simple_topics() -> Vec<&'static str> {
        vec!["/topic", "/second/topic", "/third/topic"]
    }

    fn subscribe_client(broker: &mut Broker<MessageConsumer>, client_id: &str, topics: &[&str]) {
        let topics = topics.iter().map(|str| (str.to_string(), 1)).collect();
        assert!(broker.subscribe(&client_id.to_string(), topics).is_ok());
    }

    fn register_client(broker: &mut Broker<MessageConsumer>, client_id: &str) -> Rx {
        let connect = Connect {
            version: 3,
            client_id: Some(client_id.clone().to_string()),
            auth: None,
            will: None,
            clean_session: false,
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let message_consumer = MessageConsumer::new(client_id.clone().to_string(), tx);
        let result = broker.register(&connect.client_id.unwrap(), message_consumer, None);
        assert!(result.is_ok());

        rx
    }
}
