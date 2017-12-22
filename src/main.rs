use std::net;
use std::io::{Read, Write};
use std::thread;
use std::sync::{Mutex, Arc};
use std::result;

type Result<T> = result::Result<T, &'static str>;

fn handle_client(stream: &mut net::TcpStream) {
    println!("new client: {:?}", stream);

    loop {
        let mut fixed_header = [0];
        let _ = stream.read_exact(&mut fixed_header);

        let packet_type = fixed_header[0] >> 4;
        let flags = fixed_header[0] & 0b00001111;

        println!(
            "header: {:08b} packet_type: {:08b} flags: {:08b}",
            fixed_header[0],
            packet_type,
            flags
        );

        let remaining_length = variable_length(stream);
        println!("remaining_length {:?}", remaining_length);

        let mut payload: Vec<u8> = vec![0; remaining_length as usize];
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

fn handle_connect(payload: &mut Vec<u8>) -> Result<Vec<u8>> {
    println!("connect payload {:?}", payload);
    let length = take_length(payload);
    let protocol_name = take_proto_name(payload, length);

    if protocol_name != "MQTT" {
        return Err("Invalid protocol name");
    }

    println!("protocolName: {:?} :: length: {:?}", protocol_name, length);

    Ok(vec![])
}

fn handle_unknown(payload: &mut Vec<u8>) -> Result<Vec<u8>> {
    panic!("Unknown payload: {:?}", payload)
}

fn take_proto_name(payload: &mut Vec<u8>, length: usize) -> std::string::String {
    let proto_arr: Vec<u8> = payload.drain(0..length).collect();
    String::from_utf8(proto_arr).expect("Error unwraping proto_name")
}

fn take_length(payload: &mut Vec<u8>) -> usize {
    let length_arr: Vec<u8> = payload.drain(0..2).collect();
    ((length_arr[0] as usize) << 8) | (length_arr[1] as usize)
}

fn variable_length(stream: &mut net::TcpStream) -> u8 {
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
    value
}

fn main() {
    let data = Arc::new(Mutex::new(vec![1u32, 2, 3]));

    if let Ok(listener) = net::TcpListener::bind("127.0.0.1:1883") {
        println!("Server is listening: {:?}", listener);
        for stream_result in listener.incoming() {
            let data = data.clone();
            thread::spawn(move || {
                let mut data = data.lock().unwrap();
                let mut stream = stream_result.unwrap();

                println!("Hello from a thread! {:?}", data[0]);
                handle_client(&mut stream);

                data[0] += 1;
            });
        }
    } else {
        println!("Couldn't create a listener on port 8080");
    }
}

#[test]
fn test_length() {
    assert_eq!(take_length(&mut vec![0b00000000, 0b00000000]), 0);
    assert_eq!(take_length(&mut vec![0b00000000, 0b00000001]), 1);
    assert_eq!(take_length(&mut vec![0b00000001, 0b00000000]), 256);
    assert_eq!(take_length(&mut vec![0b00000001, 0b00000001]), 257);
    assert_eq!(take_length(&mut vec![0b00000001, 0b00000001, 1]), 257);
}
