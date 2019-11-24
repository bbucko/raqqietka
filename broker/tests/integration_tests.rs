use bytes::Bytes;
use futures::executor::block_on;
use tokio::sync::mpsc;

use broker::Broker;
use core::{Connect, Publish, Subscribe};
use mqtt::{MessageConsumer, Rx};

#[test]
fn test_register() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut broker = Broker::new();
    let client_id = "client_id".to_string();
    let publisher = MessageConsumer::new(client_id.clone(), tx);
    let result = broker.register(&client_id, Box::new(publisher));

    assert!(result.is_ok());
}

#[test]
fn test_register_with_existing_client() {
    let mut broker = Broker::new();
    let client_id = "client_id".to_string();

    let (tx, mut rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.clone(), tx);
    let result = broker.register(&client_id, Box::new(publisher));

    assert!(result.is_ok());
    assert_eq!(block_on(rx.recv()).unwrap().packet_type, core::PacketType::CONNACK);

    let (tx, _rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.clone(), tx);
    let result = broker.register(&client_id, Box::new(publisher));

    assert!(result.is_ok());
    assert_eq!(block_on(rx.recv()).unwrap().packet_type, core::PacketType::DISCONNECT);
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

    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topictest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\r/second/topictest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0c/third/topictest")));
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

    assert!(broker.publish(publish(1, topic)).is_ok(), "Publish failed for topic: {}", topic);
    assert!(broker.publish(publish(2, topic)).is_ok(), "Publish failed for topic: {}", topic);

    rx.close();
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\n/topic/abctest")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\n/topic/abctest")));
    assert!(block_on(rx.recv()).is_none());
}

fn publish(packet_id: u16, topic: &str) -> Publish {
    Publish {
        packet_id: packet_id,
        topic: topic.to_owned(),
        qos: 0,
        payload: Bytes::from("test"),
    }
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
    let (tx, mut rx) = mpsc::unbounded_channel();
    let client_id = &connect.client_id.unwrap();

    let publisher = MessageConsumer::new(client_id.clone(), tx);

    assert!(broker.register(client_id, Box::new(publisher)).is_ok());
    assert_eq!(block_on(rx.recv()).unwrap().packet_type, core::PacketType::CONNACK);

    rx
}
