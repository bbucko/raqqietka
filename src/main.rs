use std::net;
use std::io::{Read, Write};
use std::thread;
use std::sync::{Mutex, Arc};
use std::result;

#[macro_use]
extern crate log;
extern crate simple_logger;

static THRESHOLD: u32 = 128 * 128 * 128;

type Result<T> = result::Result<T, &'static str>;
type MQTTPayload = Vec<u8>;

trait MQTTSupport {
    fn take_string(&mut self) -> std::string::String;
    fn take_length(&mut self) -> usize;
    fn take_one_byte(&mut self) -> u8;
    fn take_two_bytes(&mut self) -> u16;
}

impl MQTTSupport for MQTTPayload {
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
}

fn handle_client(stream: &mut net::TcpStream) -> std::result::Result<(), std::io::Error> {
    info!("new client: {:?}", stream);

    loop {
        let mut fixed_header = [0];
        let _ = stream.read_exact(&mut fixed_header)?;

        let packet_type = fixed_header[0] >> 4;
        let flags = fixed_header[0] & 0b00001111;

        info!(
            "header: {:08b} packet_type: {:08b} flags: {:08b}",
            fixed_header[0],
            packet_type,
            flags
        );

        let remaining_length = take_variable_length(stream);

        let mut payload: MQTTPayload = vec![0; remaining_length];
        stream.read_exact(&mut payload)?;

        let response = match packet_type {
            1 => handle_connect(&mut payload),
            _ => handle_unknown(&mut payload),
        };

        match response {
            Ok(data) => match stream.write(&data) {
                Ok(_) => continue,
                Err(_) => break,
            },
            Err(err_msg) => panic!("something went wrong: {:?}", err_msg),
        }
    }
    Ok(())
}

fn handle_connect(payload: &mut MQTTPayload) -> Result<Vec<u8>> {
    trace!("connect payload {:?}", payload);

    if payload.take_string() != "MQTT" {
        return Err("Invalid protocol name");
    }

    if payload.take_one_byte() != 4 {
        return Err("Invalid protocol level");
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

    info!("client_id: {:?}, username: {:?}, pwd: {:?}", client_id, _user_name, password);

    debug_assert!(payload.len() == 0);

    Ok(vec![0b00100000, 0b00000010, 0b00000000, 0b00000000])
}

fn handle_unknown(payload: &mut Vec<u8>) -> Result<Vec<u8>> {
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

    let data = Arc::new(Mutex::new(vec![1u32, 2, 3]));

    if let Ok(listener) = net::TcpListener::bind("127.0.0.1:1883") {
        info!("Server is listening: {:?}", listener);
        for stream_result in listener.incoming() {
            let data = data.clone();
            thread::spawn(move || {
                let mut data = data.lock().unwrap();
                let mut stream = stream_result.unwrap();

                trace!("Hello from a thread! {:?}", data[0]);
                let _ = handle_client(&mut stream);

                data[0] += 1;
            });
        }
    } else {
        error!("Couldn't create a listener on port 8080");
    }
}

#[test]
fn test_take_two_bytes() {
    assert_eq!(MQTTPayload::from(vec![0b00000000, 0b00000000]).take_two_bytes(), 0);
}

#[test]
fn test_take_one_byte() {
    assert_eq!(MQTTPayload::from(vec![0b00000000]).take_one_byte(), 0);
}

#[test]
fn test_is_flag_set() {
    assert!(is_flag_set(0b000000010, 1));
    assert!(is_flag_set(0b000000001, 0));
    assert!(is_flag_set(0b011111111, 0));
}

#[test]
fn test_length() {
    assert_eq!(MQTTPayload::from(vec![0b00000000, 0b00000000]).take_length(), 0);
    assert_eq!(MQTTPayload::from(vec![0b00000000, 0b00000001]).take_length(), 1);
    assert_eq!(MQTTPayload::from(vec![0b00000001, 0b00000000]).take_length(), 256);
    assert_eq!(MQTTPayload::from(vec![0b00000001, 0b00000001]).take_length(), 257);
    assert_eq!(MQTTPayload::from(vec![0b00000001, 0b00000001, 1]).take_length(), 257);
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
