use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

use crate::{Broker, ClientId, MQTTError, Packet, Publish, Subscribe, Tx, Unsubscribe};

impl Broker {
    pub fn new() -> Self {
        info!("Broker has started");

        Broker::default()
    }

    pub fn register(&mut self, client_id: &str, tx: Tx) -> Result<(), MQTTError> {
        info!("Client: {:?} has connected to broker", &client_id);

        self.clients.insert(client_id.to_owned(), tx);
        Ok(())
    }

    pub fn subscribe(&mut self, client_id: &str, subscribe: Subscribe) -> Result<Vec<u8>, MQTTError> {
        let mut result = vec![];
        for (topic, qos) in subscribe.topics {
            info!("Client: {} has subscribed to topic: {} with qos {}", client_id, topic, qos);

            Broker::validate_subscribe(&topic)?;

            self.subscriptions
                .entry(topic.to_owned())
                .or_insert_with(HashSet::new)
                .insert(client_id.to_string());

            result.push(qos);
        }

        Ok(result)
    }

    pub fn unsubscribe(&mut self, client_id: &str, unsubscribe: Unsubscribe) -> Result<(), MQTTError> {
        for topic in unsubscribe.topics.iter() {
            self.subscriptions.entry(topic.to_owned()).and_modify(|values| {
                info!("Client: {} has unsubscribed from topic: {}", client_id, topic);

                values.remove(client_id);
            });
        }

        Ok(())
    }

    pub fn validate(&self, publish: &Publish) -> Result<(), MQTTError> { Broker::validate_publish(&publish.topic) }

    pub fn publish(&mut self, publish: Publish) -> Result<(), MQTTError> {
        let subscriptions = &mut self.subscriptions;
        let clients = &mut self.clients;

        let topic = publish.topic.clone();
        let publish: Packet = publish.into();

        for (subscription, client_ips) in subscriptions {
            if Broker::filter_matches_topic(subscription, &topic) {
                client_ips.retain(|client_id| {
                    if let Some(tx) = clients.get_mut(client_id) {
                        let packet = publish.clone();
                        info!("Sending payload on topic {} to client: {}", packet, client_id);

                        tx.try_send(packet).is_ok()
                    } else {
                        info!("Removing disconnected/invalid client: {}", client_id);

                        false
                    }
                })
            }
        }

        Ok(())
    }

    pub fn acknowledge(&mut self, packet_id: u16) -> Result<(), MQTTError> {
        info!("Acknowledging packet: {}", packet_id);

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

    pub fn validate_publish(topic: &str) -> Result<(), MQTTError> {
        if topic.chars().all(|c| (char::is_alphanumeric(c) || c == '/') && c != '#' && c != '+') {
            return Ok(());
        }
        Err(format!("invalid_topic_path: {}", topic).into())
    }

    fn validate_subscribe(filter: &str) -> Result<(), String> {
        let mut peekable = filter.chars().peekable();
        loop {
            match peekable.next() {
                None => return Ok(()),
                Some('#') => {
                    if peekable.peek().is_some() {
                        return Err(format!("invalid_topic_filter: {}", filter));
                    }
                }
                Some('+') => {
                    if let Some(&next) = peekable.peek() {
                        if next != '/' {
                            return Err(format!("invalid_topic_filter: {}", filter));
                        }
                    }
                }
                Some('/') => {
                    if let Some(&next) = peekable.peek() {
                        if !(next == '+' || next == '/' || next == '#' || char::is_alphanumeric(next)) {
                            return Err(format!("invalid_topic_filter: {}", filter));
                        }
                    }
                }
                Some(_) => {
                    if let Some(&next) = peekable.peek() {
                        if !(next == '/' || char::is_alphanumeric(next)) {
                            return Err(format!("invalid_topic_filter: {}", filter));
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
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result { write!(f, "Broker(clients#: {}, subscriptions#: {})", self.clients.len(), self.subscriptions.len()) }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::StreamExt;
    use tokio::sync::mpsc;

    use crate::{Connect, Rx};

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
        futures::executor::block_on(async {
            let packets: Vec<Packet> = rx.collect().await;
            assert_eq!(packets.len(), 3);
            assert_eq!(packets[0].payload, Some(Bytes::from("\0\x06/topic\0\x01test")));
            assert_eq!(packets[1].payload, Some(Bytes::from("\0\r/second/topic\0\x01test")));
            assert_eq!(packets[2].payload, Some(Bytes::from("\0\x0c/third/topic\0\x01test")));
        });
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
        futures::executor::block_on(async {
            let packets: Vec<Packet> = rx.collect().await;
            assert_eq!(packets.len(), 1);
            assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
        });
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
        futures::executor::block_on(async {
            let packets: Vec<Packet> = rx.collect().await;
            assert_eq!(packets.len(), 1);
            assert_eq!(packets[0].payload, Some(Bytes::from("\0\x15/topic/oneLevel/first\0\x01test")));
        });
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
        futures::executor::block_on(async {
            let packets: Vec<Packet> = rx.collect().await;
            assert_eq!(packets.len(), 3);
            assert_eq!(packets[0].payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
            assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
            assert_eq!(packets[1].payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
        });
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
