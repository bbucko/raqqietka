mod handlers;

use std::io;
use std::result;
use bytes::BytesMut;
use tokio_io::codec::{Decoder, Encoder};

static THRESHOLD: u32 = 128 * 128 * 128;

#[derive(Debug)]
pub enum Type {
    CONNECT(String, Option<String>, Option<String>, Option<(String, String, u8)>),
    PUBLISH(Option<u16>, String, u8, Vec<u8>),
    SUBSCRIBE(u16, Vec<(String, u8)>),
    PINGREQ,
    DISCONNECT,
}

#[derive(Debug)]
pub struct MQTTRequest {
    pub packet: Type,
}

impl MQTTRequest {
    fn connect(client_id: String, username: Option<String>, password: Option<String>, will: Option<(String, String, u8)>) -> MQTTRequest {
        MQTTRequest { packet: Type::CONNECT(client_id, username, password, will) }
    }
    fn publish(packet_identifier: Option<u16>, topic: String, qos_level: u8, payload: Vec<u8>) -> MQTTRequest {
        MQTTRequest { packet: Type::PUBLISH(packet_identifier, topic, qos_level, payload) }
    }

    fn subscribe(packet_identifier: u16, topics: Vec<(String, u8)>) -> MQTTRequest {
        MQTTRequest { packet: Type::SUBSCRIBE(packet_identifier, topics) }
    }
    fn pingreq() -> MQTTRequest {
        MQTTRequest { packet: Type::PINGREQ }
    }
    fn disconnect() -> MQTTRequest {
        MQTTRequest { packet: Type::DISCONNECT }
    }
}

#[derive(Debug)]
pub struct MQTTResponse {}

#[derive(Debug)]
pub struct MQTTCodec;

impl Decoder for MQTTCodec {
    type Item = MQTTRequest;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> result::Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }

        match parse_fixed(src) {
            Some((first_packet, variable_length, packets_read)) => {
                src.split_to(packets_read);

                let packet_type = first_packet >> 4;
                let flags = first_packet & 0b0000_1111;
                let mut payload = src.split_to(variable_length);

                info!("packet_type: {:08b}, flags: {:08b}, payload: {:?}", packet_type, flags, payload);

                match packet_type {
                    1 => handlers::connect(&mut payload),
                    3 => handlers::publish(&mut payload, flags),
                    8 => handlers::subscribe(&mut payload),
                    12 => handlers::pingreq(&mut payload),
                    14 => handlers::disconnect(&mut payload),
                    _ => handlers::unknown(&mut payload),
                }
            }
            None => Ok(None)
        }
    }
}

impl Encoder for MQTTCodec {
    type Item = MQTTResponse;
    type Error = io::Error;

    fn encode(&mut self, _item: Self::Item, _dst: &mut BytesMut) -> result::Result<(), Self::Error> {
        Ok(())
    }
}

impl MQTTCodec {
    pub fn new() -> MQTTCodec {
        MQTTCodec {}
    }
}

fn parse_fixed(src: &[u8]) -> Option<(u8, usize, usize)> {
    match src.split_first() {
        Some((&first_packet, buf)) => {
            let (variable_length, packets_read) = take_variable_length(buf);
            Some((first_packet, variable_length, (packets_read + 1) as usize))
        }
        _ => None
    }
}

fn take_variable_length(src: &[u8]) -> (usize, u8) {
    let mut multiplier = 1;
    let mut value: u32 = 0;
    let mut iterator = src.iter();
    let mut packets_read = 0;
    loop {
        packets_read += 1;
        let encoded_byte = iterator.next().unwrap();
        value += u32::from(encoded_byte & 127) * multiplier;
        multiplier *= 128;

        if encoded_byte & 128 == 0 {
            break;
        }

        assert!(multiplier <= THRESHOLD, "malformed remaining length {}", multiplier);
    }
    (value as usize, packets_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_variable_length() {
        assert_eq!(take_variable_length(&vec![0b00000000]), (0, 1));
        assert_eq!(take_variable_length(&vec![0x7F]), (127, 1));

        assert_eq!(take_variable_length(&vec![0x80, 0x01]), (128, 2));
        assert_eq!(take_variable_length(&vec![0xFF, 0x7F]), (16_383, 2));

        assert_eq!(take_variable_length(&vec![0x80, 0x80, 0x01]), (16_384, 3));
        assert_eq!(take_variable_length(&vec![0xFF, 0xFF, 0x7F]), (2_097_151, 3));

        assert_eq!(take_variable_length(&vec![0x80, 0x80, 0x80, 0x01]), (2_097_152, 4));
        assert_eq!(take_variable_length(&vec![0xFF, 0xFF, 0xFF, 0x7F]), (268_435_455, 4));
    }

    #[test]
    #[should_panic]
    fn test_take_variable_length_malformed() {
        let _ = take_variable_length(&vec![0xFF, 0xFF, 0xFF, 0x8F]);
    }
}