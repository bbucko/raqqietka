#![feature(test)]

extern crate test;

use std::collections::HashMap;

use tokio::sync::mpsc;

use core::*;
use mqtt::{MessageConsumer, Tx};

#[bench]
fn test_publish_through_direct(b: &mut test::Bencher) {
    let (tx, _) = mpsc::unbounded_channel();
    let mut map: HashMap<String, Tx> = HashMap::new();
    map.insert("abc".to_string(), tx);

    b.iter(|| {
        let consumer = map.get_mut("abc").unwrap().clone();
        consumer.send(create_packet())
    });
}

#[bench]
fn test_publish_through_trait(b: &mut test::Bencher) {
    let (tx, _) = mpsc::unbounded_channel();
    let mut map: HashMap<String, Box<dyn Publisher>> = HashMap::new();
    map.insert("abc".to_string(), Box::new(MessageConsumer::new("abc".to_string(), tx)));

    b.iter(|| {
        let consumer = map.get_mut("abc").unwrap();
        consumer.send(create_packet())
    });
}

fn create_packet() -> Packet {
    Packet {
        packet_type: PacketType::CONNECT,
        flags: 0,
        payload: Option::None,
    }
}
