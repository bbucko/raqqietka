use std::collections::HashSet;

use bytes::Bytes;
use futures::executor::block_on;
use tokio::sync::mpsc;

use broker::Broker;
use core::*;
use mqtt::{MessageConsumer, Rx};

#[test]
fn test_register() {
    let mut broker = Broker::<MessageConsumer>::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let client_id = "client_id";
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, publisher, None);

    assert!(result.is_ok());
}

#[test]
fn test_register_with_existing_client() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let (tx, mut rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, publisher, None);
    assert!(result.is_ok());

    let (tx, _rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, publisher, None);
    assert!(result.is_ok());

    assert_eq!(block_on(rx.recv()).unwrap().packet_type, core::PacketType::DISCONNECT);
}

#[test]
fn test_register_with_lwt() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let receiver_client_id = "receiver_client_id";

    let (tx, _rx) = mpsc::unbounded_channel();
    let (tx_lwt, mut rx_lwt) = mpsc::unbounded_channel();

    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let will_message = will_message();
    let result = broker.register(&client_id, message_consumer, Some(will_message));
    assert!(result.is_ok());

    let message_consumer = MessageConsumer::new(receiver_client_id.to_owned(), tx_lwt);
    let result = broker.register(&receiver_client_id, message_consumer, None);
    assert!(result.is_ok());

    let _ = broker.subscribe(&receiver_client_id, subscribe_message(&vec!["will"]));

    broker.disconnect(client_id.to_owned());

    assert_eq!(block_on(rx_lwt.recv()).unwrap().packet_type, core::PacketType::PUBLISH);
}

#[test]
fn test_register_with_lwt_and_existing_client() {
    //GIVEN
    let mut broker = Broker::new();

    let client_id = "client_id";

    let receiver_client_id = "receiver_client_id";

    //register LWT consumer
    let (tx_lwt, mut rx_lwt) = mpsc::unbounded_channel();
    let result = broker.register(&receiver_client_id, MessageConsumer::new(receiver_client_id.to_owned(), tx_lwt), None);
    assert!(result.is_ok());

    let _ = broker.subscribe(&receiver_client_id, subscribe_message(&vec!["will"]));

    //connect client with LWT
    let (tx, _rx) = mpsc::unbounded_channel();
    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, message_consumer, Some(will_message()));
    assert!(result.is_ok());

    //disconnect this client
    broker.disconnect(client_id.to_owned());

    assert_eq!(block_on(rx_lwt.recv()).unwrap().packet_type, core::PacketType::PUBLISH);

    //WHEN
    //reconnect client with LWT
    let (tx, _rx) = mpsc::unbounded_channel();
    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, message_consumer, Some(will_message()));

    //THEN
    assert!(result.is_ok());
}

#[test]
fn test_broker_qos0_publish_with_plain_subscription() {
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

    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topictest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\r/second/topictest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0c/third/topictest")));
    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_qos1_publish_with_plain_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic", "/second/topic", "/third/topic"]);

    for topic in vec!["/topic", "/second/topic", "/third/topic"] {
        let publish = Publish {
            packet_id: 1,
            topic: topic.to_owned(),
            qos: 1,
            payload: Bytes::from("test"),
        };
        assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
    }

    rx.close();

    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topic\0\0test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\r/second/topic\0\0test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0c/third/topic\0\0test")));
    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_qos1_multiple_publish_with_plain_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let topic = "/topic";
    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic"]);

    for packet_id in 0..3 {
        let publish = Publish {
            packet_id,
            topic: topic.to_owned(),
            qos: 1,
            payload: Bytes::from("test"),
        };
        assert!(broker.publish(publish).is_ok(), "Publish failed for topic: {}", topic);
    }

    rx.close();

    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topic\0\x00test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topic\0\x01test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topic\0\x02test")));

    assert!(block_on(rx.recv()).is_none());
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0f/topic/oneLeveltest")));
    assert!(block_on(rx.recv()).is_none());
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x15/topic/oneLevel/firsttest")));
    assert!(block_on(rx.recv()).is_none());
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0f/topic/oneLeveltest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x11/topic/two/levelstest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x15/topic/oneLevel/firsttest")));
    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_publish_increasing_counter() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    let topic = "/topic/abc";
    subscribe_client(&mut broker, client_id, &[topic]);

    assert!(broker.publish(publish_message(1, topic)).is_ok(), "Publish failed for topic: {}", topic);
    assert!(broker.publish(publish_message(2, topic)).is_ok(), "Publish failed for topic: {}", topic);

    rx.close();
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\n/topic/abctest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\n/topic/abctest")));
    assert!(block_on(rx.recv()).is_none());
}

fn subscribe_client(broker: &mut Broker<MessageConsumer>, client_id: &str, topics: &[&str]) {
    let subscribe = subscribe_message(topics);

    assert!(broker.subscribe(&client_id.to_owned(), subscribe).is_ok());
}

fn register_client(broker: &mut Broker<MessageConsumer>, client_id: &str) -> Rx {
    let connect = Connect {
        version: 3,
        client_id: Some(client_id.to_string()),
        auth: None,
        will: None,
        clean_session: false,
    };

    let (tx, rx) = mpsc::unbounded_channel();

    let client_id = &connect.client_id.unwrap();
    let publisher = MessageConsumer::new(client_id.clone(), tx);
    let result = broker.register(client_id, publisher, None);
    assert!(result.is_ok());

    rx
}

fn publish_message(packet_id: u16, topic: &str) -> Publish {
    Publish {
        packet_id,
        topic: topic.to_owned(),
        qos: 0,
        payload: Bytes::from("test"),
    }
}

fn subscribe_message(topics: &[&str]) -> Subscribe {
    let topics: HashSet<(Topic, u8)> = topics.iter().map(|str| (str.to_string(), 1)).collect();
    Subscribe { packet_id: 0, topics }
}

fn will_message() -> Will {
    Will {
        qos: 1,
        retain: false,
        topic: "will".to_string(),
        message: Bytes::from("payload"),
    }
}
