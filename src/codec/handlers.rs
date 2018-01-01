extern crate core;

use std::io;
use std::result;
use std::str;
use std::string;
use std::slice;

pub type Error = io::Error;
pub type Result = result::Result<Option<super::MQTTRequest>, Error>;

pub fn connect(payload: &[u8]) -> Result {
    trace!("CONNECT payload {:?}", payload);
    let mut iter = payload.iter();

    let proto_name = take_string(&mut iter)?;
    if proto_name != "MQTT" {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol name: {}", proto_name)));
    }

    let proto_level = take_one_byte(&mut iter);
    if proto_level != 4 {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol version: {}", proto_level)));
    }

    let connect_flags = take_one_byte(&mut iter);
    let _keep_alive = take_two_bytes(&mut iter);
    let client_id = take_string(&mut iter);

    let will = if is_flag_set(connect_flags, 2) {
        Some((take_string(&mut iter)?, take_string(&mut iter)?, (connect_flags & 0b0001_1000) >> 4))
    } else {
        None
    };

    let username = if is_flag_set(connect_flags, 7) {
        Some(take_string(&mut iter)?)
    } else {
        None
    };
    let password = if is_flag_set(connect_flags, 6) {
        Some(take_string(&mut iter)?)
    } else {
        None
    };

    info!("connected client_id: {:?}, username: {:?}, pwd: {:?}", client_id, username, password);
    Ok(Some(super::MQTTRequest::connect(client_id?, username, password, will)))
}

pub fn publish(payload: &[u8], flags: u8) -> Result {
    trace!("PUBLISH payload {:?}", payload);
    let mut iter = payload.iter();

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b0000_0011;
    info!("dup: {} retain: {} qos_level: {}", dup, retain, qos_level);

    let topic_name = take_string(&mut iter)?;

    match qos_level {
        0 => {
            let msg = take_payload(&mut iter);
            info!("publishing payload: {:?} on topic: '{}'", msg, topic_name);

            Ok(Some(super::MQTTRequest::publish(None, topic_name, qos_level, msg)))
        }
        1 | 2 => {
            let packet_identifier = take_two_bytes(&mut iter);
            let msg = take_payload(&mut iter);
            info!("publishing payload: {:?} on topic: '{}' in response to packet_id: {}", msg, topic_name, packet_identifier);

            Ok(Some(super::MQTTRequest::publish(Some(packet_identifier), topic_name, qos_level, msg)))
        }
        _ => Err(io::Error::new(io::ErrorKind::Other, "invalid qos")),
    }
}

pub fn subscribe(payload: &[u8]) -> Result {
    trace!("SUBSCRIBE payload {:?}", payload);
    let mut iter = payload.iter();

    let packet_identifier = take_two_bytes(&mut iter);

    let mut topics = Vec::new();
    loop {
        let new_topic = match (iter.next(), iter.next()) {
            (None, None) => break,
            (Some(first), Some(second)) => {
                let length = (u16::from(*first) >> 8) | u16::from(*second);
                let topic_name = (0..length).map(|_| *iter.next().unwrap()).collect::<Vec<u8>>();
                let qos = *iter.next().unwrap();
                Ok((str::from_utf8(&topic_name).unwrap().to_string(), qos))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "malformed packet"))
        }?;
        topics.push(new_topic);
    }

    info!("Responding to packet id: {}", packet_identifier);
    Ok(Some(super::MQTTRequest::subscribe(packet_identifier, topics)))
}

pub fn pingreq(payload: &[u8]) -> Result {
    trace!("PINGREQ payload {:?}", payload);

    Ok(Some(super::MQTTRequest::pingreq()))
}

pub fn disconnect(payload: &[u8]) -> Result {
    trace!("DISCONNECT payload {:?}", payload);

    Ok(Some(super::MQTTRequest::disconnect()))
}

fn take_string(payload: &mut slice::Iter<u8>) -> result::Result<string::String, io::Error> {
    let length: usize = match (payload.next(), payload.next()) {
        (Some(first), Some(second)) => Ok((usize::from(*first) << 8) | usize::from(*second)),
        _ => Err(io::Error::new(io::ErrorKind::Other, "malformed packet"))
    }?;
    let a = (0..length).map(|_| *payload.next().expect("")).collect::<Vec<u8>>();
    Ok(str::from_utf8(&a).map_err(|_| io::Error::new(io::ErrorKind::Other, "malformed utf8"))?.to_string())
}

fn take_one_byte(payload: &mut slice::Iter<u8>) -> u8 {
    *payload.next().unwrap()
}

fn take_two_bytes(payload: &mut slice::Iter<u8>) -> u16 {
    (u16::from(*payload.next().unwrap()) << 8) | u16::from(*payload.next().unwrap())
}

fn take_payload(payload: &mut slice::Iter<u8>) -> Vec<u8> {
    payload.map(|x| *x).collect::<Vec<u8>>()
}

fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 0b0000_0001 == 0b0000_0001
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;

    #[test]
    fn test_parse_connect_with_client_id_only() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Ok(Some(MQTTRequest { packet: Type::CONNECT(client_id, None, None, None), })) => assert_eq!(client_id, "abc"),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_connect_with_client_id_and_username_and_password() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 194, 0, 60, 0, 3, 97, 98, 99, 0, 8, 117, 115, 101, 114, 110, 97, 109, 101, 0, 8, 112, 97, 115, 115, 119, 111, 114, 100])) {
            Ok(Some(MQTTRequest { packet: Type::CONNECT(client_id, Some(a), Some(b), None), })) => {
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
        match publish(&mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99]), 0b00000000) {
            Ok(Some(MQTTRequest { packet: Type::PUBLISH(None, topic, qos_level, payload), })) => {
                assert_eq!(topic, "/something");
                assert_eq!(qos_level, 0);
                assert_eq!(payload, Vec::from("abc"));
            }
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos1() {
        match publish(&mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 0, 1, 97, 98, 99]), 0b00000010) {
            Ok(Some(MQTTRequest { packet: Type::PUBLISH(Some(packet_id), topic, qos_level, payload), })) => {
                assert_eq!(packet_id, 1);
                assert_eq!(topic, "/something");
                assert_eq!(qos_level, 1);
                assert_eq!(payload, vec![97, 98, 99]);
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
        assert_eq!(take_string(&mut vec![0, 3, 97, 98, 99].iter()), "abc");
    }

    #[test]
    fn test_take_two_bytes() {
        assert_eq!(take_two_bytes(&mut vec![0, 0].iter()), 0);
    }

    #[test]
    fn test_take_one_byte() {
        assert_eq!(take_one_byte(&mut vec![0].iter()), 0);
    }

    fn test_something() {}
}
