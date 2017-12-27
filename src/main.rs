mod helpers;

use helpers::*;
use std::io;
use std::io::Write;
use std::thread;
use std::net;
use std::result;

#[macro_use]
extern crate log;
extern crate simple_logger;

#[derive(Debug)]
pub enum MQTTError {
    Io(io::Error),
    Generic(&'static str),
}

#[derive(Debug, PartialEq)]
pub enum Action {
    Respond(Vec<u8>),
    Continue,
    Disconnect,
}


pub type MQTTResult<T> = result::Result<T, MQTTError>;

fn handle_client(stream: &mut net::TcpStream) -> MQTTResult<()> {
    info!("new client connected: {:?}", stream);

    loop {
        let response = handle_packet(stream)?;
        trace!("response: {:?}", response);

        let written_bytes = match response {
            Action::Continue => continue,
            Action::Disconnect => return Ok(()),
            Action::Respond(data) => stream.write(&data).map_err(MQTTError::Io)?,
        };

        trace!("written bytes: {}", written_bytes);
    }
}

fn handle_packet(mut stream: &mut io::Read) -> MQTTResult<Action> {
    let mut buf = [0];
    stream.read_exact(&mut buf).map_err(MQTTError::Io)?;

    let fixed_header = buf[0];

    let packet_type = fixed_header >> 4;
    let flags = fixed_header & 0b00001111;

    info!("header: {:08b} packet_type: {} flags: {:08b}", fixed_header, packet_type, flags);
    let remaining_length = stream.take_variable_length();
    let mut payload = vec![0; remaining_length];
    stream.read_exact(&mut payload).map_err(MQTTError::Io)?;

    match packet_type {
        1 => handle_connect(&mut payload, flags),
        3 => handle_publish(&mut payload, flags),
        8 => handle_subscribe(&mut payload, flags),
        12 => handle_pingreq(&mut payload, flags),
        14 => handle_disconnect(&mut payload, flags),
        _ => handle_unknown(&mut payload),
    }
}

fn handle_connect(payload: &mut Vec<u8>, _flags: u8) -> MQTTResult<Action> {
    trace!("CONNECT payload {:?}", payload);

    if payload.take_string() != "MQTT" {
        return Err(MQTTError::Generic("Invalid proto name"));
    }

    if payload.take_one_byte() != 4 {
        return Err(MQTTError::Generic("Invalid version"));
    }

    let connect_flags = payload.take_one_byte();
    let _keep_alive = payload.take_two_bytes();

    let client_id = payload.take_string();

    if is_flag_set(connect_flags, 2) {
        let _will = (
            payload.take_string(),
            payload.take_string(),
            (connect_flags & 0b00011000) >> 4,
        );
        info!("will: {:?}", _will);
    }

    let _user_name = match is_flag_set(connect_flags, 7) {
        true => payload.take_string(),
        false => String::from("n/a"),
    };

    let password = match is_flag_set(connect_flags, 6) {
        true => payload.take_string(),
        false => String::from("n/a"),
    };

    debug_assert!(payload.len() == 0);

    info!("connected client_id: {}, username: {}, pwd: {}", client_id, _user_name, password);

    Ok(Action::Respond(vec![0b00100000, 0b00000010, 0b00000000, 0b00000000]))
}

fn handle_publish(payload: &mut Vec<u8>, flags: u8) -> MQTTResult<Action> {
    trace!("PUBLISH payload {:?}", payload);

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b00000011;
    info!("dup: {} retain: {} qos_level: {}", dup, retain, qos_level);

    let topic_name = payload.take_string();

    match qos_level {
        0 => {
            let msg = payload.take_payload();
            info!("publishing payload: {:?} on topic: {}", msg, topic_name);
            Ok(Action::Continue)
        }
        1 | 2 => {
            let packet_identifier = payload.take_two_bytes();
            let msg = payload.take_payload();
            info!("publishing payload: {:?} on topic: {} in response to packet_id: {}", msg, topic_name, packet_identifier);

            Ok(Action::Respond(vec![0b01000000, 0b00000010, (packet_identifier >> 8) as u8, packet_identifier as u8, ]))
        }
        _ => Err(MQTTError::Generic("Invalid QOS")),
    }
}

fn handle_subscribe(payload: &mut Vec<u8>, _flags: u8) -> MQTTResult<Action> {
    trace!("SUBSCRIBE payload {:?}", payload);

    let packet_identifier = payload.take_two_bytes();

    let mut topics = Vec::new();
    while payload.len() > 0 {
        let topic_filter = payload.take_string();
        let topic_qos = payload.take_one_byte();

        info!("filter {} :: qos {}", topic_filter, topic_qos);

        topics.push((topic_filter, topic_qos));
    }

    info!("Responding to packet id: {}", packet_identifier);
    let mut response = vec![0b10010000, 2 + topics.len() as u8, (packet_identifier >> 8) as u8, packet_identifier as u8, ];

    for (_, topic_qos) in topics {
        response.push(topic_qos);
    }

    Ok(Action::Respond(response))
}

fn handle_pingreq(payload: &mut Vec<u8>, _flags: u8) -> MQTTResult<Action> {
    trace!("PINGREQ payload {:?}", payload);

    Ok(Action::Respond(vec![0b11010000, 0b00000000]))
}

fn handle_disconnect(payload: &mut Vec<u8>, _flags: u8) -> MQTTResult<Action> {
    trace!("DISCONNECT payload {:?}", payload);

    Ok(Action::Disconnect)
}

fn handle_unknown(payload: &mut Vec<u8>) -> MQTTResult<Action> {
    panic!("Unknown payload: {:?}", payload)
}

pub fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 0b00000001 == 0b00000001
}


fn main() {
    simple_logger::init().unwrap();

    if let Ok(listener) = net::TcpListener::bind("127.0.0.1:1883") {
        info!("Server is listening: {:?}", listener);
        for stream_result in listener.incoming() {
            thread::spawn(move || {
                let mut stream = stream_result.unwrap();
                let _ = handle_client(&mut stream);
            });
        }
    } else {
        error!("Couldn't create a listener on port 8080");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_flag_set() {
        assert!(is_flag_set(0b000000010, 1));
        assert!(is_flag_set(0b000000001, 0));
        assert!(is_flag_set(0b011111111, 0));
    }

    #[test]
    #[should_panic]
    fn test_unknown_packet() {
        let _ = handle_packet(&mut std::io::Cursor::new(vec![0b00000000]));
    }

    #[test]
    fn test_parse_connect() {
        match handle_packet(&mut std::io::Cursor::new(vec![16, 15, 0, 4, 77, 81, 84, 84, 4, 2, 0, 60, 0, 3, 97, 98, 99])) {
            Ok(Action::Respond(data)) => assert_eq!(data, vec![32, 2, 0, 0]),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos0() {
        match handle_packet(&mut std::io::Cursor::new(vec![48, 14, 0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99])) {
            Ok(Action::Continue) => assert!(true),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_parse_publish_qos1() {
        match handle_packet(&mut std::io::Cursor::new(vec![50, 14, 0, 10, 47, 115, 111, 109, 101, 116, 104, 105, 110, 103, 97, 98, 99])) {
            Ok(Action::Respond(data)) => assert_eq!(data, vec![64, 2, 97, 98]),
            _ => assert!(false),
        }
    }
}
