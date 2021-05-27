use bytes::Bytes;
use futures::executor::block_on;
use tokio::sync::mpsc;

use broker::Broker;
use core::*;
use mqtt::{MessageConsumer, Rx};
use mqttrs::{Connect, Protocol, QoS};
use std::convert::TryFrom;

#[test]
fn test_register() {
    let mut broker = Broker::<MessageConsumer>::new();

    let (tx, _rx) = mpsc::unbounded_channel();
    let client_id = "client_id";
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(client_id, publisher, None);

    assert!(result.is_ok());
}

#[test]
fn test_register_with_existing_client() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let (tx, _rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, publisher, None);
    assert!(result.is_ok());

    let (tx, _rx) = mpsc::unbounded_channel();
    let publisher = MessageConsumer::new(client_id.to_owned(), tx);
    let result = broker.register(&client_id, publisher, None);
    assert!(result.is_ok());

    //    assert!(rx.is_closed());
}

#[test]
fn test_forced_disconnect_with_lwt() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let receiver_client_id = "receiver_client_id";
    let will_message_topic = "/will/topic";

    let (tx, _rx) = mpsc::unbounded_channel();
    let (tx_lwt, mut rx_lwt) = mpsc::unbounded_channel();

    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let will_message = will_message(will_message_topic.to_string());
    let result = broker.register(&client_id, message_consumer, Some(will_message));
    assert!(result.is_ok());

    let message_consumer = MessageConsumer::new(receiver_client_id.to_owned(), tx_lwt);
    let result = broker.register(&receiver_client_id, message_consumer, None);
    assert!(result.is_ok());

    let _ = broker.subscribe(&receiver_client_id, subscribe_message(&vec![will_message_topic]));

    broker.cleanup(client_id);

    let result = block_on(rx_lwt.recv()).unwrap();
    if let mqttrs::Packet::Publish(publish) = result {
        assert_eq!(publish.topic_name, will_message_topic);
    } else {
        panic!("should not be here");
    }
}

#[test]
fn test_clean_disconnect_with_lwt() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let receiver_client_id = "receiver_client_id";
    let will_message_topic = "/will/topic";

    let (tx, _rx) = mpsc::unbounded_channel();
    let (tx_lwt, mut rx_lwt) = mpsc::unbounded_channel();

    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let will_message = will_message(will_message_topic.to_string());
    let result = broker.register(&client_id, message_consumer, Some(will_message));
    assert!(result.is_ok());

    let message_consumer = MessageConsumer::new(receiver_client_id.to_owned(), tx_lwt);
    let result = broker.register(&receiver_client_id, message_consumer, None);
    assert!(result.is_ok());

    let _ = broker.subscribe(&receiver_client_id, subscribe_message(&vec![will_message_topic]));

    broker.cleanup(client_id);

    let result = block_on(rx_lwt.recv()).unwrap();
    if let mqttrs::Packet::Publish(publish) = result {
        assert_eq!(publish.topic_name, will_message_topic);
    } else {
        panic!("should not be here");
    }
}

#[test]
fn test_forced_disconnect_with_lwt_and_existing_client() {
    //GIVEN
    let mut broker = Broker::new();

    let client_id = "client_id";

    let receiver_client_id = "receiver_client_id";
    let will_message_topic = "/will/topic";

    //register LWT consumer
    let (tx_lwt, mut rx_lwt) = mpsc::unbounded_channel();
    let result = broker.register(&receiver_client_id, MessageConsumer::new(receiver_client_id.to_owned(), tx_lwt), None);
    assert!(result.is_ok());

    let _ = broker.subscribe(&receiver_client_id, subscribe_message(&vec![will_message_topic]));

    //connect client with LWT
    let (tx, _rx) = mpsc::unbounded_channel();
    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let will_msg = will_message(will_message_topic.to_string());

    let result = broker.register(&client_id, message_consumer, Some(will_msg.into()));
    assert!(result.is_ok());

    //disconnect this client
    broker.cleanup(client_id);

    let result = block_on(rx_lwt.recv()).unwrap();
    if let mqttrs::Packet::Publish(publish) = result {
        assert_eq!(publish.topic_name, will_message_topic);
    } else {
        panic!("should not be here");
    }

    //WHEN
    //reconnect client with LWT
    let (tx, _rx) = mpsc::unbounded_channel();
    let message_consumer = MessageConsumer::new(client_id.to_owned(), tx);
    let will_message = will_message(will_message_topic.to_string());

    let result = broker.register(&client_id, message_consumer, Some(will_message.into()));

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
        assert!(
            broker.publish(topic.to_string(), 0, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();

    for topic in vec!["/topic", "/second/topic", "/third/topic"] {
        if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
            assert_eq!(publish.payload, "test".as_bytes());
            assert_eq!(publish.topic_name, topic);
            assert_eq!(publish.qospid.pid(), None);
        }
    }

    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_qos1_publish_with_plain_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic", "/second/topic", "/third/topic"]);

    for topic in vec!["/topic", "/second/topic", "/third/topic"] {
        assert!(
            broker.publish(topic.to_string(), 1, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();

    for topic in vec!["/topic", "/second/topic", "/third/topic"] {
        if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
            assert_eq!(publish.payload, "test".as_bytes());
            assert_eq!(publish.topic_name, topic);
            assert_eq!(publish.qospid.qos(), QoS::AtLeastOnce);
            assert_eq!(publish.qospid.pid().unwrap(), mqttrs::Pid::new());
        }
    }
    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_qos1_multiple_publish_with_plain_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let topic = "/topic";
    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic"]);

    for _packet_id in 1..4 {
        assert!(
            broker.publish(topic.to_string(), 1, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();

    for packet_id in 1..4 {
        if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
            assert_eq!(publish.payload, "test".as_bytes());
            assert_eq!(publish.topic_name, "/topic");
            assert_eq!(publish.qospid.pid().unwrap(), mqttrs::Pid::try_from(packet_id).unwrap());
        }
    }

    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_publish_with_wildcard_one_level_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic/+"]);

    for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/level/first", "/different/topic"] {
        assert!(
            broker.publish(topic.to_string(), 0, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();
    if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
        assert_eq!(publish.payload, "test".as_bytes());
        assert_eq!(publish.topic_name, "/topic/oneLevel");
    }

    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_publish_with_wildcard_one_level_in_the_middle_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic/+/first"]);

    for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first", "/different/topic"] {
        assert!(
            broker.publish(topic.to_string(), 0, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();

    if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
        assert_eq!(publish.payload, "test".as_bytes());
        assert_eq!(publish.topic_name, "/topic/oneLevel/first");
    }

    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_publish_with_wildcard_multilevel_at_the_end_subscription() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    subscribe_client(&mut broker, client_id, &["/topic/#"]);

    for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first", "/different/topic"] {
        assert!(
            broker.publish(topic.to_string(), 0, Bytes::from("test")).is_ok(),
            "Publish failed for topic: {}",
            topic
        );
    }

    rx.close();
    for topic in vec!["/topic/oneLevel", "/topic/two/levels", "/topic/oneLevel/first"] {
        if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
            assert_eq!(publish.payload, "test".as_bytes());
            assert_eq!(publish.topic_name, topic);
        }
    }

    assert!(block_on(rx.recv()).is_none());
}

#[test]
fn test_broker_publish_increasing_counter() {
    let mut broker = Broker::new();

    let client_id = "client_id";

    let mut rx = register_client(&mut broker, client_id);
    let topic = "/topic/abc";
    subscribe_client(&mut broker, client_id, &[topic]);

    assert!(
        broker.publish(topic.to_string(), 1, Bytes::from("test")).is_ok(),
        "Publish failed for topic: {}",
        topic
    );
    assert!(
        broker.publish(topic.to_string(), 1, Bytes::from("test")).is_ok(),
        "Publish failed for topic: {}",
        topic
    );

    rx.close();

    if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
        assert_eq!(publish.payload, "test".as_bytes());
        assert_eq!(publish.topic_name, "/topic/abc");
        assert_eq!(publish.qospid.pid().unwrap(), mqttrs::Pid::try_from(1).unwrap());
    }

    if let mqttrs::Packet::Publish(publish) = block_on(rx.recv()).unwrap() {
        assert_eq!(publish.payload, "test".as_bytes());
        assert_eq!(publish.topic_name, "/topic/abc");
        assert_eq!(publish.qospid.pid().unwrap(), mqttrs::Pid::try_from(2).unwrap());
    }

    assert!(block_on(rx.recv()).is_none());
}

fn subscribe_client(broker: &mut Broker<MessageConsumer>, client_id: &str, topics: &[&str]) {
    let subscribe = subscribe_message(topics);

    assert!(broker.subscribe(&client_id.to_owned(), subscribe).is_ok());
}

fn register_client(broker: &mut Broker<MessageConsumer>, client_id: &str) -> Rx {
    let connect = Connect {
        protocol: Protocol::MQTT311,
        keep_alive: 0,
        client_id: client_id.to_string(),
        clean_session: false,
        last_will: Option::None,
        username: Option::None,
        password: Option::None,
    };

    let (tx, rx) = mpsc::unbounded_channel();

    let client_id = ClientId::from(&connect.client_id);
    let publisher = MessageConsumer::new(client_id.clone(), tx);
    let result = broker.register(&client_id, publisher, None);
    assert!(result.is_ok());

    rx
}

fn subscribe_message(topics: &[&str]) -> Vec<(Topic, Qos)> {
    topics.iter().map(|str| (str.to_string(), 1)).collect()
}

fn will_message(topic: String) -> Message {
    Message {
        id: 0,
        payload: Bytes::from(""),
        topic: topic,
        qos: 0,
    }
}
