mod handlers;

use std::io;
use std::result;
use bytes::BytesMut;
use tokio_io::codec::{Decoder, Encoder};
use bytes::{BufMut, BigEndian};
use futures::{future, Future};
use tokio_service::Service;

static THRESHOLD: u32 = 128 * 128 * 128;

#[derive(Debug)]
pub enum Type {
    CONNECT(String, Option<String>, Option<String>, Option<(String, String, u8)>),
    CONACK,
    PUBLISH(Option<u16>, String, u8, Vec<u8>),
    PUBACK(u16),
    SUBSCRIBE(u16, Vec<(String, u8)>),
    SUBACK(u16, Vec<u8>),
    PINGREQ,
    PINGRES,
    DISCONNECT,
    NONE,
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
pub struct MQTTResponse {
    pub packet: Type,
}

impl MQTTResponse {
    pub fn connack() -> MQTTResponse {
        MQTTResponse { packet: Type::CONACK }
    }

    pub fn puback(packet_identifier: u16) -> MQTTResponse {
        MQTTResponse { packet: Type::PUBACK(packet_identifier) }
    }

    pub fn suback(packet_identifier: u16, qos: Vec<u8>) -> MQTTResponse {
        MQTTResponse { packet: Type::SUBACK(packet_identifier, qos) }
    }

    pub fn none() -> MQTTResponse {
        MQTTResponse { packet: Type::NONE }
    }
}

impl Decoder for super::MQTTCodec {
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
                    _ => panic!("Unknown payload: {:?}", payload)
                }
            }
            None => Ok(None)
        }
    }
}

impl Encoder for super::MQTTCodec {
    type Item = MQTTResponse;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> result::Result<(), Self::Error> {
        match item.packet {
            Type::CONACK => dst.put(vec![0b0010_0000, 0b0000_0010, 0b0000_0001, 0b0000_0000]),
            Type::PUBACK(packet_identifier) => {
                dst.put(vec![0b0100_0000, 0b0000_0010]);
                dst.put_u16::<BigEndian>(packet_identifier);
            }
            Type::SUBACK(packet_identifier, qos) => {
                dst.put(vec![0b01001_0000, 0b0000_0010]);
                dst.put_u16::<BigEndian>(packet_identifier);
                dst.put(qos);
            }
            Type::NONE | Type::DISCONNECT => return Ok(()),
            _ => panic!("Unknown packet: {:?}", item)
        }
        info!("{:?}", dst);
        Ok(())
    }
}

impl super::MQTTCodec {
    pub fn new() -> super::MQTTCodec {
        super::MQTTCodec {}
    }
}

impl Service for super::MQTT {
    // These types must match the corresponding protocol types:
    type Request = MQTTRequest;
    type Response = MQTTResponse;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        // In this case, the response is immediate.
        info!("Request: {:?}", req);
        let response = match req.packet {
            Type::CONNECT(_client_id, _username, _password, _will) => MQTTResponse::connack(),
            Type::PUBLISH(Some(packet_identifier), _topic, _qos_level, _payload) => MQTTResponse::puback(packet_identifier),
            Type::PUBLISH(None, _topic, 0, _payload) => MQTTResponse::none(),
            Type::SUBSCRIBE(packet_identifier, topics) => MQTTResponse::suback(packet_identifier, topics.iter().map(|topic| topic.1).collect()),
            Type::DISCONNECT => MQTTResponse::none(),
            _ => panic!("unknown packet")
        };

        Box::new(future::ok(response))
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