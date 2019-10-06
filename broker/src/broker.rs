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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result { write!(f, "Broker(clients#: {}, subscriptions#: {})", self.clients.len(), self.subscriptions.len()) }
}
