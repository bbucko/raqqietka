use std::net;
use std::io::{Read, Write};
use std::thread;
use std::result;

#[macro_use]
extern crate log;
extern crate simple_logger;

static THRESHOLD: u32 = 128 * 128 * 128;

#[derive(Debug)]
enum MQTTError {
    Io(std::io::Error),
    Error(),
}

type MQTTResult<T> = result::Result<T, MQTTError>;
type Payload = Vec<u8>;

#[derive(Debug, PartialEq)]
enum Action {
    Respond(Vec<u8>),
    Disconnect,
    Continue,
}

trait MQTTSupport {
    fn take_string(&mut self) -> std::string::String;
    fn take_length(&mut self) -> usize;
    fn take_one_byte(&mut self) -> u8;
    fn take_two_bytes(&mut self) -> u16;
    fn take_payload(&mut self) -> Vec<u8>;
}

impl MQTTSupport for Payload {
    fn take_string(&mut self) -> std::string::String {
        let length = self.take_length();
        let proto_name: Vec<u8> = self.drain(0..length).collect();
        String::from_utf8(proto_name).expect("Error unwrapping string")
    }

    fn take_length(&mut self) -> usize {
        let length: Vec<u8> = self.drain(0..2).collect();
        ((length[0] as usize) << 8) | (length[1] as usize)
    }

    fn take_one_byte(&mut self) -> u8 {
        let take_flags: Vec<u8> = self.drain(0..1).collect();
        take_flags[0]
    }

    fn take_two_bytes(&mut self) -> u16 {
        let keep_alive: Vec<u8> = self.drain(0..2).collect();
        ((keep_alive[0] as u16) << 8) | (keep_alive[1] as u16)
    }

    fn take_payload(&mut self) -> Vec<u8> {
        self.drain(0..).collect()
    }
}

fn handle_client(stream: &mut net::TcpStream) -> MQTTResult<()> {
    info!("new client: {:?}", stream);

    loop {
        let mut fixed_header = [0];
        stream.read_exact(&mut fixed_header).map_err(MQTTError::Io)?;

        let packet_type = fixed_header[0] >> 4;
        let flags = fixed_header[0] & 0b00001111;

        info!("header: {:08b} packet_type: {:?} flags: {:08b}", fixed_header[0], packet_type, flags);

        let remaining_length = take_variable_length(stream);

        let mut payload: Payload = vec![0; remaining_length];
        stream.read_exact(&mut payload).map_err(MQTTError::Io)?;

        let response = match packet_type {
            1 => handle_connect(&mut payload, flags),
            3 => handle_publish(&mut payload, flags),
            8 => handle_subscribe(&mut payload, flags),
            12 => handle_pingreq(&mut payload, flags),
            14 => handle_disconnect(&mut payload, flags),
            _ => handle_unknown(&mut payload),
        }?;

        trace!("response: {:?}", response);

        let written_bytes = match response {
            Action::Continue => continue,
            Action::Disconnect => return Ok(()),
            Action::Respond(data) => stream.write(&data).map_err(MQTTError::Io)?,
        };

        trace!("written bytes: {:?}", written_bytes);
    }
}

fn handle_connect(payload: &mut Payload, _flags: u8) -> MQTTResult<Action> {
    trace!("CONNECT payload {:?}", payload);

    if payload.take_string() != "MQTT" {
        return Err(MQTTError::Error());
    }

    if payload.take_one_byte() != 4 {
        return Err(MQTTError::Error());
    }

    let connect_flags = payload.take_one_byte();
    let _keep_alive = payload.take_two_bytes();

    let client_id = payload.take_string();

    if is_flag_set(connect_flags, 2) {
        let _will = (payload.take_string(), payload.take_string(), (connect_flags & 0b00011000) >> 4);
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

    info!("connected client_id: {:?}, username: {:?}, pwd: {:?}", client_id, _user_name, password);

    Ok(Action::Respond(vec![0b00100000, 0b00000010, 0b00000000, 0b00000000]))
}

fn handle_publish(payload: &mut Payload, flags: u8) -> MQTTResult<Action> {
    trace!("PUBLISH payload {:?}", payload);

    let dup = is_flag_set(flags, 3);
    let retain = is_flag_set(flags, 0);
    let qos_level = (flags >> 1) & 0b00000011;
    info!("dup: {:?} retain: {:?} qos_level: {:?}", dup, retain, qos_level);

    let topic_name = payload.take_string();

    if qos_level > 0 {
        let packet_identifier = payload.take_two_bytes();
        info!("Responding to packet id: {:?}", packet_identifier);

        let msg = payload.take_payload();
        info!("publishing payload: {:?} on topic: {:?}", msg, topic_name);

        return Ok(Action::Respond(vec![0b01000000, 0b00000010, (packet_identifier >> 8) as u8, packet_identifier as u8]));
    }

    let msg = payload.take_payload();
    info!("publishing payload: {:?} on topic: {:?}", msg, topic_name);
    Ok(Action::Continue)
}

fn handle_subscribe(payload: &mut Payload, _flags: u8) -> MQTTResult<Action> {
    trace!("SUBSCRIBE payload {:?}", payload);

    let packet_identifier = payload.take_two_bytes();

    let mut topics = Vec::new();
    while payload.len() > 0 {
        let topic_filter = payload.take_string();
        let topic_qos = payload.take_one_byte();

        info!("filter {:?} :: qos {:?}", topic_filter, topic_qos);

        topics.push((topic_filter, topic_qos));
    }

    info!("Responding to packet id: {:?}", packet_identifier);
    let mut response = vec![0b10010000, 2 + topics.len() as u8, (packet_identifier >> 8) as u8, packet_identifier as u8];

    for (_, topic_qos) in topics {
        response.push(topic_qos);
    }

    Ok(Action::Respond(response))
}

fn handle_pingreq(payload: &mut Payload, _flags: u8) -> MQTTResult<Action> {
    trace!("PINGREQ payload {:?}", payload);

    Ok(Action::Respond(vec![0b11010000, 0b00000000]))
}

fn handle_disconnect(payload: &mut Payload, _flags: u8) -> MQTTResult<Action> {
    trace!("DISCONNECT payload {:?}", payload);

    Ok(Action::Disconnect)
}

fn handle_unknown(payload: &mut Vec<u8>) -> MQTTResult<Action> {
    panic!("Unknown payload: {:?}", payload)
}

fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 0b00000001 == 0b00000001
}

fn take_variable_length(stream: &mut Read) -> usize {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;
    let mut encoded_byte = [0];

    loop {
        let _ = stream.read_exact(&mut encoded_byte);
        value += (encoded_byte[0] & 127) as u32 * multiplier;
        multiplier *= 128;

        if encoded_byte[0] & 128 == 0 {
            break;
        }

        if multiplier > THRESHOLD {
            panic!("malformed remaining length {:?}", multiplier);
        }
    }
    info!("length: {:?}", value);
    value as usize
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

#[test]
fn test_take_two_bytes() {
    assert_eq!(Payload::from(vec![0b00000000, 0b00000000]).take_two_bytes(), 0);
}

#[test]
fn test_take_one_byte() {
    assert_eq!(Payload::from(vec![0b00000000]).take_one_byte(), 0);
}

#[test]
fn test_is_flag_set() {
    assert!(is_flag_set(0b000000010, 1));
    assert!(is_flag_set(0b000000001, 0));
    assert!(is_flag_set(0b011111111, 0));
}

#[test]
fn test_length() {
    assert_eq!(Payload::from(vec![0b00000000, 0b00000000]).take_length(), 0);
    assert_eq!(Payload::from(vec![0b00000000, 0b00000001]).take_length(), 1);
    assert_eq!(Payload::from(vec![0b00000001, 0b00000000]).take_length(), 256);
    assert_eq!(Payload::from(vec![0b00000001, 0b00000001]).take_length(), 257);
    assert_eq!(Payload::from(vec![0b00000001, 0b00000001, 1]).take_length(), 257);
}

#[test]
fn test_take_variable_length() {
    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0b00000000])), 0);
    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0x7F])), 127);

    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0x80, 0x01])), 128);
    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0xFF, 0x7F])), 16_383);

    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0x80, 0x80, 0x01])), 16_384);
    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0xFF, 0xFF, 0x7F])), 2_097_151);

    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0x80, 0x80, 0x80, 0x01])), 2_097_152);
    assert_eq!(take_variable_length(&mut std::io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0x7F])), 268_435_455);
}

#[test]
#[should_panic]
fn test_take_variable_length_malformed() {
    take_variable_length(&mut std::io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0x8F]));
}
