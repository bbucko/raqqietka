use std::io;
use std::result;
use std::str;
use std::string;

pub type Error = io::Error;
pub type Result = result::Result<Option<super::MQTTRequest>, Error>;

pub fn connect(payload: &[u8]) -> Result {
    trace!("CONNECT payload {:?}", payload);
    let mut pos = 0;

    let proto_name = take_string(payload, &mut pos);
    if proto_name != "MQTT" {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol name: {}", proto_name)));
    }

    let proto_level = take_one_byte(payload, &mut pos);
    if proto_level != 4 {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol version: {}", proto_level)));
    }

    let connect_flags = take_one_byte(payload, &mut pos);
    let _keep_alive = take_two_bytes(payload, &mut pos);
    let client_id = take_string(payload, &mut pos);

    let will = if is_flag_set(connect_flags, 2) {
        Some((take_string(payload, &mut pos), take_string(payload, &mut pos), (connect_flags & 0b0001_1000) >> 4))
    } else {
        None
    };

    let username = if is_flag_set(connect_flags, 7) {
        Some(take_string(payload, &mut pos))
    } else {
        None
    };
    let password = if is_flag_set(connect_flags, 6) {
        Some(take_string(payload, &mut pos))
    } else {
        None
    };

    info!("connected client_id: {:?}, username: {:?}, pwd: {:?}", client_id, username, password);
    Ok(Some(super::MQTTRequest::connect(client_id, username, password, will)))
}

pub fn publish(payload: &[u8], flags: u8) -> Result {
    trace!("PUBLISH payload {:?}", payload);
    let mut pos = 0;

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b0000_0011;
    info!("dup: {} retain: {} qos_level: {}", dup, retain, qos_level);

    let topic_name = take_string(payload, &mut pos);

    match qos_level {
        0 => {
            let msg = take_payload(payload, &mut pos);
            info!("publishing payload: {:?} on topic: '{}'", msg, topic_name);

            Ok(Some(super::MQTTRequest::publish(None, topic_name, qos_level, msg)))
        }
        1 | 2 => {
            let packet_identifier = take_two_bytes(payload, &mut pos);
            let msg = take_payload(payload, &mut pos);
            info!("publishing payload: {:?} on topic: '{}' in response to packet_id: {}", msg, topic_name, packet_identifier);

            Ok(Some(super::MQTTRequest::publish(Some(packet_identifier), topic_name, qos_level, msg)))
        }
        _ => Err(io::Error::new(io::ErrorKind::Other, "invalid qos")),
    }
}

pub fn subscribe(payload: &[u8]) -> Result {
    trace!("SUBSCRIBE payload {:?}", payload);
    let mut pos = 0;

    let packet_identifier = take_two_bytes(payload, &mut pos);

    let mut topics = Vec::new();
    while !pos < payload.len() {
        let topic_filter = take_string(payload, &mut pos);
        let topic_qos = take_one_byte(payload, &mut pos);

        info!("filter {} :: qos {}", topic_filter, topic_qos);

        topics.push((topic_filter, topic_qos));
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

fn take_string(payload: &[u8], pos: &mut usize) -> string::String {
    let length = take_length(payload, pos);
    let proto_name = &payload[*pos..*pos + length];
    *pos += length;
    str::from_utf8(proto_name).unwrap().to_string()
}

fn take_length(payload: &[u8], pos: &mut usize) -> usize {
    let bytes = &payload[*pos..*pos + 2];
    let length = ((bytes[0] as usize) << 8) | (bytes[1] as usize);
    *pos += 2;
    length
}

fn take_one_byte(src: &[u8], pos: &mut usize) -> u8 {
    let payload = src[*pos];
    *pos += 1;
    payload
}

fn take_two_bytes(payload: &[u8], pos: &mut usize) -> u16 {
    let bytes = &payload[*pos..*pos + 2];
    *pos += 2;
    (u16::from(bytes[0]) << 8) | u16::from(bytes[1])
}

fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 0b0000_0001 == 0b0000_0001
}

fn take_payload(src: &[u8], pos: &mut usize) -> Vec<u8> {
    let payload = Vec::from(&src[*pos..]);
    *pos += payload.len();
    payload
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
    fn test_take_two_bytes() {
        let mut pos = 0;
        assert_eq!(take_two_bytes(&mut vec![0, 0], &mut pos), 0);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_take_one_byte() {
        let mut pos = 0;
        assert_eq!(take_one_byte(&mut vec![0], &mut pos), 0);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_length() {
        let mut pos = 0;
        assert_eq!(take_length(&mut vec![0, 0], &mut pos), 0);
        assert_eq!(pos, 2);

        assert_eq!(take_length(&mut vec![0, 1], &mut 0), 1);
        assert_eq!(take_length(&mut vec![1, 0], &mut 0), 256);
        assert_eq!(take_length(&mut vec![1, 1], &mut 0), 257);
        assert_eq!(take_length(&mut vec![1, 1, 1], &mut 0), 257);
    }
}
