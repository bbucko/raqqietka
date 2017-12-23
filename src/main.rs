use std::net;
use std::io::{Read, Write};
use std::thread;
use std::sync::{Mutex, Arc};
use std::result;

#[macro_use]
extern crate log;
extern crate simple_logger;

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

fn handle_client(stream: &mut net::TcpStream) {
    info!("new client: {:?}", stream);

    loop {
        let mut fixed_header = [0];
        let _ = stream.read_exact(&mut fixed_header);

        let packet_type = fixed_header[0] >> 4;
        let flags = fixed_header[0] & 0b00001111;

        info!(
            "header: {:08b} packet_type: {:08b} flags: {:08b}",
            fixed_header[0],
            packet_type,
            flags
        );

        let remaining_length = take_variable_length(stream);

        let mut payload = MQTTPayload::with_capacity(remaining_length);
        let _ = stream.read_exact(&mut payload);

        let response = match packet_type {
            1 => handle_connect(&mut payload),
            _ => handle_unknown(&mut payload),
        };

        match response {
            Ok(result) => {
                match stream.write(&result) {
                    Err(_) => break,
                    Ok(_) => continue,
                }
            }
            Err(err_msg) => panic!("something went wrong: {:?}", err_msg),
        }
    }
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

    let password = match is_flag_set(connect_flags, 6) {
        true => payload.take_string(),
        false => String::from("n/a"),
    };

    info!("client_id {:?}, password {:?}", client_id, password);

    debug_assert!(payload.len() == 0);

    Ok(vec![0b00100000, 0b00000010, 0b00000000, 0b00000000])
}

fn handle_unknown(payload: &mut Vec<u8>) -> Result<Vec<u8>> {
    panic!("Unknown payload: {:?}", payload)
}

fn is_flag_set(connect_flags: u8, pos: u8) -> bool {
    (connect_flags >> pos) & 1 == 1
}

fn take_variable_length(stream: &mut Read) -> usize {
    let mut multiplier = 1;
    let mut value: u8 = 0;
    let mut encoded_byte = [0];

    let _ = stream.read_exact(&mut encoded_byte);
    value += (encoded_byte[0] & 127) * multiplier;
    multiplier *= 128;

    while encoded_byte[0] & 128 != 0 {
        let _ = stream.read_exact(&mut encoded_byte);
        value += (encoded_byte[0] & 127) * multiplier;
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            panic!();
        }
    }
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
                handle_client(&mut stream);

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
fn test_take_variable_length() {}
