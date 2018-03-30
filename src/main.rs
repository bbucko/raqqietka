#![deny(warnings)]
extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate tokio;

mod client;
mod codec;

use codec::MQTTCodec;
use client::Client;

use bytes::BytesMut;
use futures::{Future, Stream};
use futures::future::{self, Either};

use log::LogLevel;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
pub struct Broker {}

impl Broker {
    pub fn new() -> Broker {
        Broker {}
    }
}

#[derive(Debug)]
pub struct Packet {
    packet_type: u8,
    flags: u8,
    payload: BytesMut,
}

impl Packet {
    fn new(header: u8, payload: BytesMut) -> Self {
        let packet_type = header >> 4;
        let flags = header & 0b0000_1111;
        Packet { packet_type, flags, payload }
    }
}

fn handle_error(e: io::Error) {
    error!("connection error = {:?}", e);
}

fn process(socket: TcpStream, broker: Arc<Mutex<Broker>>) -> Box<Future<Item = (), Error = ()> + Send> {
    info!("new connection accepted from: {:?} to broker: {:?}", socket.peer_addr(), broker);

    let msg = MQTTCodec::new(socket)
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(|(connect, packets)| {
            info!("new client connected: {:?}", connect);

            match connect {
                Some(connect) => {
                    if let Some(client) = Client::new(&connect.payload, packets) {
                        return Either::A(client);
                    } else {
                        return Either::B(future::ok(()));
                    }
                }
                None => Either::B(future::ok(())),
            }
        })
        .map_err(handle_error);

    Box::new(msg)
}

fn main() {
    simple_logger::init_with_level(LogLevel::Info).unwrap();
    info!("raqqietka starting");

    let broker = Arc::new(Mutex::new(Broker::new()));

    let addr = "127.0.0.1:1883".parse().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();
    let server = listener
        .incoming()
        .map_err(|e| error!("failed to accept socket; error = {:?}", e))
        .for_each(move |socket| tokio::spawn(process(socket, broker.clone())));

    tokio::run(server);
}
