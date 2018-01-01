mod handlers;

use bytes::BytesMut;
use std::io;
use std::result;
use tokio_io::codec::{Decoder, Encoder};

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

        info!("src: {:?}", src);
        match parse_fixed(src) {
            Some((first_packet, variable_length, packets_read)) => {
                let packet_type = first_packet >> 4;
                let flags = first_packet & 0b0000_1111;

                if src.len() < packets_read + variable_length {
                    return Ok(None);
                }

                info!("src: {:?}, src.len: {}, variable_length: {}, packets_read: {}", src, src.len(), variable_length, packets_read);

                let payload = &src.split_to(packets_read + variable_length)[packets_read..];

                info!("packet_type: {:08b}, flags: {:08b}, payload: {:?}", packet_type, flags, payload);

                match packet_type {
                    1 => handlers::connect(payload),
                    3 => handlers::publish(payload, flags),
                    8 => handlers::subscribe(payload),
                    12 => handlers::pingreq(payload),
                    14 => handlers::disconnect(payload),
                    _ => panic!("Unknown payload: {:?}", payload),
                }
            }
            None => Ok(None),
        }
    }
}

impl Encoder for super::MQTTCodec {
    type Item = MQTTResponse;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> result::Result<(), Self::Error> {
        match item.packet {
            Type::CONACK => {
                dst.extend(vec![0b0010_0000, 0b0000_0010, 0b0000_0001, 0b0000_0000]);
                Ok(())
            }
            Type::PUBACK(packet_identifier) => {
                dst.extend(vec![0b0100_0000, 0b0000_0010]);
                dst.extend(vec![(packet_identifier >> 8) as u8, packet_identifier as u8, ]);
                Ok(())
            }
            Type::SUBACK(packet_identifier, qos) => {
                info!("packet_identifier: {:?}, qos: {:?}", packet_identifier, qos);
                dst.extend(vec![0b1001_0000, 0b0000_0010]);
                dst.extend(vec![(packet_identifier >> 8) as u8, packet_identifier as u8, ]);
                dst.extend(qos);
                Ok(())
            }
            Type::NONE => Ok(()),
            Type::DISCONNECT => Err(io::Error::new(io::ErrorKind::Other, "disconnect")),
            _ => Err(io::Error::new(io::ErrorKind::Other, "unknown packet")),
        }
    }
}

fn parse_fixed(src: &[u8]) -> Option<(u8, usize, usize)> {
    let (variable_length, packets_read) = take_variable_length(&src[1..]);
    info!("variable: {}: packets_read: {}", variable_length, packets_read);
    Some((src[0], variable_length, (packets_read + 1) as usize))
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

        assert_eq!(take_variable_length(&vec![0x80, 0x80, 0x80, 0x01]), (2_097_152, 4, ));
        assert_eq!(take_variable_length(&vec![0xFF, 0xFF, 0xFF, 0x7F]), (268_435_455, 4, ));
    }

    #[test]
    #[should_panic]
    fn test_take_variable_length_malformed() {
        let _ = take_variable_length(&vec![0xFF, 0xFF, 0xFF, 0x8F]);
    }
}
