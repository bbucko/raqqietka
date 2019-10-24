use bytes::Bytes;

use broker::Broker;
use core::{Publish, Subscribe, Connect};
use futures::executor::block_on;
use tokio::sync::mpsc;
use mqtt::{MQTTPublisher, Rx};

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

    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x06/topic\0\x01test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\r/second/topic\0\x01test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0c/third/topic\0\x01test")));
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x15/topic/oneLevel/first\0\x01test")));
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
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x0f/topic/oneLevel\0\x01test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x11/topic/two/levels\0\x01test")));
    assert_eq!(block_on(rx.recv()).unwrap().payload, Some(Bytes::from("\0\x15/topic/oneLevel/first\0\x01test")));
    assert!(block_on(rx.recv()).is_none());
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
    let client_id = &connect.client_id.unwrap();

    let publisher = MQTTPublisher::new(client_id.clone(), tx);

    assert!(broker.register(client_id, Box::new(publisher)).is_ok());

    rx
}