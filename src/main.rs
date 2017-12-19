use std::net;
use std::io::{Read, Write};
use std::thread;
use std::sync::{Mutex, Arc};

#[derive(Debug)]
struct PacketHeader {}

impl PacketHeader {
    fn new() -> PacketHeader {
        PacketHeader {}
    }
}

#[derive(Debug)]
struct Packet {
    header: PacketHeader,
}

impl Packet {
    fn new() -> Packet {
        Packet { header: PacketHeader::new() }
    }
}

fn handle_client(stream: &mut net::TcpStream) {
    println!("new client: {:?}", stream);

    loop {
        let mut packet_type = [0];
        let _ = stream.read_exact(&mut packet_type);

        let mut flags = [0, 0];
        let _ = stream.read_exact(&mut flags);
        println!("data: {:?}, flags: {:?}", packet_type, flags);

        let packet = Packet::new();
        println!("packet: {:?}", packet);

        let response = [97, 97, 97, 13, 10];
        match stream.write(&response) {
            Err(_) => break,
            Ok(_) => continue,
        }
    }
}

fn main() {
    let data = Arc::new(Mutex::new(vec![1u32, 2, 3]));

    if let Ok(listener) = net::TcpListener::bind("127.0.0.1:8080") {
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
