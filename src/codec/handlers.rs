use std::io;
use std::string;
use std::result;
use bytes::BytesMut;


pub type Error = io::Error;
pub type Result = result::Result<Option<super::MQTTRequest>, Error>;

pub fn connect(payload: &mut BytesMut) -> Result {
    trace!("CONNECT payload {:?}", payload);

    let proto_name = take_string(payload);
    if proto_name != "MQTT" {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol name: {}", proto_name)));
    }

    let proto_level = take_one_byte(payload);
    if proto_level != 4 {
        return Err(io::Error::new(io::ErrorKind::Other, format!("Invalid protocol version: {}", proto_level)));
    }

    let connect_flags = take_one_byte(payload);
    let _keep_alive = take_two_bytes(payload);

    let client_id = take_string(payload);


    let will = if is_flag_set(connect_flags, 2) {
        Some((take_string(payload), take_string(payload), (connect_flags & 0b0001_1000) >> 4))
    } else {
        None
    };


    let username = if is_flag_set(connect_flags, 7) { Some(take_string(payload)) } else { None };
    let password = if is_flag_set(connect_flags, 6) { Some(take_string(payload)) } else { None };

    info!("connected client_id: {:?}, username: {:?}, pwd: {:?}", client_id, username, password);
    Ok(Some(super::MQTTRequest::connect(client_id, username, password, will)))
}

pub fn unknown(payload: &mut BytesMut) -> Result {
    panic!("Unknown payload: {:?}", payload)
}

pub fn publish(payload: &mut BytesMut, flags: u8) -> Result {
    trace!("PUBLISH payload {:?}", payload);

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b0000_0011;
    info!("dup: {} retain: {} qos_level: {}", dup, retain, qos_level);

    let topic_name = take_string(payload);

    match qos_level {
        0 => {
            let msg = take_payload(payload);
            info!("publishing payload: {:?} on topic: '{}'", msg, topic_name);

            assert!(payload.is_empty(), "payload: {:?}", payload);
            Ok(None)
        }
        1 | 2 => {
            let packet_identifier = take_two_bytes(payload);
            let msg = take_payload(payload);
            info!("publishing payload: {:?} on topic: '{}' in response to packet_id: {}", msg, topic_name, packet_identifier);

            assert!(payload.is_empty(), "payload: {:?}", payload);
            Ok(Some(super::MQTTRequest::publish()))
//            Ok(Some(vec![0b0100_0000, 0b0000_0010, (packet_identifier >> 8) as u8, packet_identifier as u8, ]))
        }
        _ => Err(io::Error::new(io::ErrorKind::Other, "invalid qos"))
    }
}

pub fn subscribe(payload: &mut BytesMut) -> Result {
    trace!("SUBSCRIBE payload {:?}", payload);

    let packet_identifier = take_two_bytes(payload);

    let mut topics = Vec::new();
    while !payload.is_empty() {
        let topic_filter = take_string(payload);
        let topic_qos = take_one_byte(payload);

        info!("filter {} :: qos {}", topic_filter, topic_qos);

        topics.push((topic_filter, topic_qos));
    }

    info!("Responding to packet id: {}", packet_identifier);
    let mut response = vec![0b1001_0000, 2 + topics.len() as u8, (packet_identifier >> 8) as u8, packet_identifier as u8, ];

    for (_, topic_qos) in topics {
        response.push(topic_qos);
    }

    assert!(payload.is_empty(), "payload: {:?}", payload);
    Ok(Some(super::MQTTRequest::subscribe()))
}

pub fn pingreq(payload: &mut BytesMut) -> Result {
    trace!("PINGREQ payload {:?}", payload);

    assert_eq!(payload.len(), 0);
//    Ok(Some(vec![0b1101_0000, 0b0000_0000]))
    Ok(Some(super::MQTTRequest::pingreq()))
}

pub fn disconnect(payload: &mut BytesMut) -> Result {
    trace!("DISCONNECT payload {:?}", payload);

    assert_eq!(payload.len(), 0);
//    Ok(Action::Disconnect)
    Ok(Some(super::MQTTRequest::disconnect()))
}

fn take_string(payload: &mut BytesMut) -> string::String {
    let length = take_length(payload);
    let proto_name = payload.split_to(length).to_vec();
    String::from_utf8(proto_name).expect("Error unwrapping string")
}

fn take_length(payload: &mut BytesMut) -> usize {
    let bytes = payload.split_to(2);
    ((bytes[0] as usize) << 8) | (bytes[1] as usize)
}

fn take_one_byte(payload: &mut BytesMut) -> u8 {
    payload.split_to(1)[0]
}

fn take_two_bytes(payload: &mut BytesMut) -> u16 {
    let bytes = payload.split_to(2);
    ((bytes[0] as u16) << 8) | (bytes[1] as u16)
}

fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 0b0000_0001 == 0b0000_0001
}

fn take_payload(payload: &mut BytesMut) -> Vec<u8> {
    let length = payload.len();
    payload.split_to(length).to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;

    #[test]
    fn test_parse_connect_with_client_id_only() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Ok(Some(MQTTRequest { packet: Type::CONNECT(client_id, None, None, None) })) => assert_eq!(client_id, "abc"),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_connect_with_client_id_and_username_and_password() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 4, 194, 0, 60, 0, 3, 97, 98, 99, 0, 8, 117, 115, 101, 114, 110, 97, 109, 101, 0, 8, 112, 97, 115, 115, 119, 111, 114, 100])) {
            Ok(Some(MQTTRequest { packet: Type::CONNECT(client_id, Some(a), Some(b), None) })) => {
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
            _ => assert!(false)
        }
    }

    #[test]
    fn test_parse_connect_invalid_version() {
        match connect(&mut BytesMut::from(vec![0, 4, 77, 81, 84, 84, 1, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Err(err) => assert_eq!(err.to_string(), "Invalid protocol version: 1"),
            _ => assert!(false)
        }
    }

    #[test]
    fn test_parse_publish_qos0() {
        match publish(&mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99]), 0b00000000) {
            Ok(None) => assert!(true),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos1() {
        match publish(&mut BytesMut::from(vec![0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99]), 0b00000010) {
            Ok(Some(MQTTRequest { packet: Type::PUBLISH })) => assert_eq!(true, true),
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
        assert_eq!(take_two_bytes(&mut BytesMut::from(vec![0, 0])), 0);
    }

    #[test]
    fn test_take_one_byte() {
        assert_eq!(take_one_byte(&mut BytesMut::from(vec![0])), 0);
    }

    #[test]
    fn test_length() {
        assert_eq!(take_length(&mut BytesMut::from(vec![0, 0])), 0);
        assert_eq!(take_length(&mut BytesMut::from(vec![0, 1])), 1);
        assert_eq!(take_length(&mut BytesMut::from(vec![1, 0])), 256);
        assert_eq!(take_length(&mut BytesMut::from(vec![1, 1])), 257);
        assert_eq!(take_length(&mut BytesMut::from(vec![1, 1, 1])), 257);
    }
}