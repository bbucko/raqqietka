#![feature(test)]

use core::{Packet, PacketType, Publisher};
use mqtt::{MessageConsumer, Tx};
use std::collections::HashMap;
use tokio::sync::mpsc;

extern crate test;

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

fn create_packet() -> Packet {
    Packet {
        packet_type: PacketType::CONNECT,
        flags: 0,
        payload: Option::None,
    }
}
