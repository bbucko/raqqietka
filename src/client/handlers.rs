extern crate core;

use std::io;
use std::result;
use std::slice;
use std::string;

pub type Error = io::Error;
pub type Result<T> = result::Result<T, Error>;
pub type Rs = Result<Option<MQTTPacket>>;

struct Errors;

#[derive(Debug)]
pub enum Type {
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
pub struct MQTTPacket {
    pub packet: Type,
}

impl MQTTPacket {
    pub fn publish(packet_identifier: Option<u16>, topic: String, qos_level: u8, payload: Vec<u8>) -> MQTTPacket {
        MQTTPacket {
            packet: Type::PUBLISH(packet_identifier, topic, qos_level, payload),
        }
    }

    pub fn subscribe(packet_identifier: u16, topics: Vec<(String, u8)>) -> MQTTPacket {
        MQTTPacket {
            packet: Type::SUBSCRIBE(packet_identifier, topics),
        }
    }

    pub fn pingreq() -> MQTTPacket { MQTTPacket { packet: Type::PINGREQ } }

    pub fn disconnect() -> MQTTPacket { MQTTPacket { packet: Type::DISCONNECT } }

    pub fn puback(packet_identifier: u16) -> MQTTPacket {
        MQTTPacket {
            packet: Type::PUBACK(packet_identifier),
        }
    }

    pub fn suback(packet_identifier: u16, qos: Vec<u8>) -> MQTTPacket {
        MQTTPacket {
            packet: Type::SUBACK(packet_identifier, qos),
        }
    }

    pub fn pingres() -> MQTTPacket { MQTTPacket { packet: Type::PINGRES } }

    pub fn none() -> MQTTPacket { MQTTPacket { packet: Type::NONE } }
}

impl Errors {
    fn custom(error_message: String) -> Error { Error::new(io::ErrorKind::Other, error_message) }

    fn malformed_packet() -> Error { Error::new(io::ErrorKind::Other, "malformed packet") }

    fn malformed_utf8() -> Error { Error::new(io::ErrorKind::Other, "malformed UTF-8") }

    fn invalid_qos() -> Error { Error::new(io::ErrorKind::Other, "invalid QOS") }
}

pub fn connect(payload: &[u8]) -> Result<Option<(String, Option<String>, Option<String>, Option<(String, String, u8)>)>> {
    trace!("CONNECT payload {:?}", payload);
    let mut iter = payload.iter();

    let proto_name = take_string(&mut iter)?;
    if proto_name != "MQTT" {
        error!("Invalid protocol name: {}", proto_name);
        return Err(Errors::custom(format!("Invalid protocol name: {}", proto_name)));
    }

    let proto_level = take_one_byte(&mut iter)?;
    if proto_level != 4 {
        error!("Invalid protocol level: {}", proto_level);
        return Err(Errors::custom(format!("Invalid protocol version: {}", proto_level)));
    }

    let connect_flags = take_one_byte(&mut iter)?;
    let _keep_alive = take_two_bytes(&mut iter)?;
    let client_id = take_string(&mut iter)?;

    let will = if is_flag_set(connect_flags, 2) {
        Some((take_string(&mut iter)?, take_string(&mut iter)?, (connect_flags & 0b0001_1000) >> 4))
    } else {
        None
    };

    let username = if is_flag_set(connect_flags, 7) { Some(take_string(&mut iter)?) } else { None };
    let password = if is_flag_set(connect_flags, 6) { Some(take_string(&mut iter)?) } else { None };

    info!("connected client_id: {:?}; username: {:?}; pwd: {:?}", client_id, username, password);
    Ok(Some((client_id, username, password, will)))
}

pub fn publish(payload: &[u8], flags: u8) -> Rs {
    trace!("PUBLISH payload {:?}", payload);
    let mut iter = payload.iter();

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b0000_0011;
    info!("dup: {}; retain: {}; qos_level: {}", dup, retain, qos_level);

    let topic_name = take_string(&mut iter)?;

    match qos_level {
        0 => {
            let msg = take_payload(&mut iter);
            info!("publishing payload: {:?} on topic: '{}'", msg, topic_name);

            Ok(Some(MQTTPacket::publish(None, topic_name, qos_level, msg)))
        }
        1 | 2 => {
            let packet_identifier = take_two_bytes(&mut iter)?;
            let msg = take_payload(&mut iter);
            info!(
                "publishing payload: {:?} on topic: '{}' in response to packet_id: {}",
                msg, topic_name, packet_identifier
            );

            Ok(Some(MQTTPacket::publish(Some(packet_identifier), topic_name, qos_level, msg)))
        }
        _ => Err(Errors::invalid_qos()),
    }
}

pub fn subscribe(payload: &[u8]) -> Rs {
    trace!("SUBSCRIBE payload {:?}", payload);
    let mut iter = payload.iter();

    let packet_identifier = take_two_bytes(&mut iter)?;

    let mut topics = Vec::new();
    loop {
        let new_topic = match (iter.next(), iter.next()) {
            (None, None) => break,
            (Some(first), Some(second)) => {
                let length = (usize::from(*first) >> 8) | usize::from(*second);
                let topic_name = take_string_of_length(&mut iter, length)?;
                let qos = *iter.next().unwrap();
                Ok((topic_name, qos))
            }
            _ => Err(Errors::malformed_packet()),
        }?;
        topics.push(new_topic);
    }

    info!("Responding to packet id: {}", packet_identifier);
    Ok(Some(MQTTPacket::subscribe(packet_identifier, topics)))
}

pub fn pingreq(payload: &[u8]) -> Rs {
    trace!("PINGREQ payload {:?}", payload);

    Ok(Some(MQTTPacket::pingreq()))
}

pub fn disconnect(payload: &[u8]) -> Rs {
    trace!("DISCONNECT payload {:?}", payload);

    Ok(Some(MQTTPacket::disconnect()))
}

fn take_string(payload: &mut slice::Iter<u8>) -> Result<string::String> {
    let length: usize = match (payload.next(), payload.next()) {
        (Some(first), Some(second)) => Ok((usize::from(*first) << 8) | usize::from(*second)),
        _ => Err(Errors::malformed_packet()),
    }?;

    take_string_of_length(payload, length)
}

fn take_string_of_length(payload: &mut slice::Iter<u8>, length: usize) -> Result<String> {
    let string_vector = payload.take(length).cloned().collect::<Vec<u8>>();
    match string_vector.len() {
        x if x == length => string::String::from_utf8(string_vector).map_err(|_| Errors::malformed_utf8()),
        _ => Err(Errors::malformed_packet()),
    }
}

fn take_one_byte(payload: &mut slice::Iter<u8>) -> Result<u8> {
    match payload.next() {
        Some(data) => Ok(*data),
        _ => Err(Errors::malformed_packet()),
    }
}

fn take_two_bytes(payload: &mut slice::Iter<u8>) -> Result<u16> {
    match (payload.next(), payload.next()) {
        (Some(first), Some(second)) => Ok((u16::from(*first) << 8) | u16::from(*second)),
        _ => Err(Errors::malformed_packet()),
    }
}

fn take_payload(payload: &mut slice::Iter<u8>) -> Vec<u8> { payload.cloned().collect::<Vec<u8>>() }

fn is_flag_set(connect_flags: u8, pos: u8) -> bool { (connect_flags >> pos) & 0b0000_0001 == 0b0000_0001 }

#[cfg(test)]
mod tests {
    // use test::Bencher;
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_parse_connect_with_client_id_only() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Ok(Some((client_id, None, None, None))) => assert_eq!(client_id, "abc"),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_connect_with_client_id_and_username_and_password() {
        match connect(&mut BytesMut::from(vec![
            0, 4, 77, 81, 84, 84, 4, 194, 0, 60, 0, 3, 97, 98, 99, 0, 8, 117, 115, 101, 114, 110, 97, 109, 101, 0, 8, 112, 97, 115, 115, 119, 111, 114, 100
        ])) {
            Ok(Some((client_id, Some(a), Some(b), None))) => {
                assert_eq!(client_id, "abc");
                assert_eq!(a, "username");
                assert_eq!(b, "password");
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_connect_invalid_proto() {
        match connect(&mut BytesMut::from(vec![0, 4, 97, 98, 99, 100, 4, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Err(err) => assert_eq!(err.to_string(), "Invalid protocol name: abcd"),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_connect_invalid_version() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 1, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Err(err) => assert_eq!(err.to_string(), "Invalid protocol version: 1"),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos0() {
        match publish(
            &mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99]),
            0b00000000,
        ) {
            Ok(Some(MQTTPacket {
                packet: Type::PUBLISH(None, topic, qos_level, payload),
            })) => {
                assert_eq!(topic, "/something");
                assert_eq!(qos_level, 0);
                assert_eq!(payload, Vec::from("abc"));
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos1() {
        match publish(
            &mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 0, 1, 97, 98, 99]),
            0b00000010,
        ) {
            Ok(Some(MQTTPacket {
                packet: Type::PUBLISH(Some(packet_id), topic, qos_level, payload),
            })) => {
                assert_eq!(packet_id, 1);
                assert_eq!(topic, "/something");
                assert_eq!(qos_level, 1);
                assert_eq!(payload, vec![97, 98, 99]);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_subscribe() {
        match subscribe(&mut BytesMut::from(vec![0, 1, 0, 1, 97, 0, 0, 3, 97, 98, 99, 1])) {
            Ok(Some(MQTTPacket {
                packet: Type::SUBSCRIBE(packet_id, topics),
            })) => {
                assert_eq!(packet_id, 1);
                assert_eq!(topics.len(), 2);

                assert_eq!(topics[0].0, "a");
                assert_eq!(topics[0].1, 0);

                assert_eq!(topics[1].0, "abc");
                assert_eq!(topics[1].1, 1);
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_is_flag_set() {
        assert!(is_flag_set(0b0000_00010, 1));
        assert!(is_flag_set(0b0000_00001, 0));
        assert!(is_flag_set(0b0111_11111, 0));
    }

    #[test]
    fn test_take_string() {
        match take_string(&mut vec![0, 3, 97, 98, 99].iter()) {
            Ok(str) => assert_eq!(str, "abc".to_string()),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_take_malformed_string() {
        match take_string(&mut vec![0, 3, 97, 98].iter()) {
            Err(err) => assert_eq!(err.to_string(), "malformed packet"),
            Ok(data) => assert_eq!(data, "ab"),
        }
    }

    #[test]
    fn test_take_two_bytes() {
        match take_two_bytes(&mut vec![0, 0].iter()) {
            Ok(data) => assert_eq!(data, 0),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_take_one_byte() {
        match take_one_byte(&mut vec![0].iter()) {
            Ok(data) => assert_eq!(data, 0),
            _ => assert!(false),
        }
    }

    // #[bench]
    // fn bench_publish(b: &mut Bencher) {
    //     let payload = BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 0, 1, 97, 98, 99]);
    //     b.iter(|| publish(&payload, 0))
    // }
    //
    //    #[bench]
    //    fn bench_connect(b: &mut Bencher) {
    //        let payload = BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 194, 0, 60, 0, 3, 97, 98, 99, 0, 8, 117, 115, 101, 114, 110, 97, 109, 101, 0, 8, 112, 97, 115, 115, 119, 111, 114, 100, ]);
    //        b.iter(|| {
    //            connect(&payload)
    //        })
    //    }
    //
    //    #[bench]
    //    fn bench_subscribe(b: &mut Bencher) {
    //        let payload = BytesMut::from(vec![0, 1, 0, 1, 97, 0, 0, 3, 97, 98, 99, 1]);
    //        b.iter(|| {
    //            subscribe(&payload)
    //        })
    //    }
}
