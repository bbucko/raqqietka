#![deny(warnings)]

extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate tokio;

use bytes::BytesMut;
use futures::{Future, Stream};
use log::LogLevel;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

static THRESHOLD: u32 = 128 * 128 * 128;

#[derive(Debug)]
pub struct Client {
    packets: MQTTCodec,
    client_info: String,
}

impl Future for Client {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        while let Async::Ready(packet) = self.packets.poll()? {
            if let Some(mqtt_packet) = packet {
                //handle various packets
                info!("Received parsed packet ({:?}) : {:?}", self, mqtt_packet);
            } else {
                return Ok(Async::Ready(()));
            }
        }
        Ok(Async::NotReady)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Disconecting");
    }
}

impl Client {
    pub fn new(client_info: String, packets: MQTTCodec) -> Self {
        Client {
            client_info: client_info,
            packets: packets,
        }
    }
}

#[derive(Debug)]
pub struct Broker {}

impl Broker {
    pub fn new() -> Broker { Broker {} }
}

#[derive(Debug)]
pub struct Packet {}

#[derive(Debug)]
pub struct MQTTCodec {
    stream: TcpStream,
    rd: BytesMut,
    wr: BytesMut,
}

impl MQTTCodec {
    fn new(stream: TcpStream) -> Self {
        MQTTCodec {
            stream: stream,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    fn fill_read_buf(&mut self) -> Poll<(), io::Error> {
        loop {
            // Ensure the read buffer has capacity.
            self.rd.reserve(1024);

            // Read data into the buffer.
            let n = try_ready!(self.stream.read_buf(&mut self.rd));
            if n == 0 {
                debug!("Closing socket");
                return Ok(Async::Ready(()));
            }
        }
    }

    fn take_variable_length(&mut self) -> Option<usize> {
        let mut multiplier = 1;
        let mut value: u32 = 0;

        loop {
            let buf = self.rd.split_to(1);
            if buf.is_empty() {
                return None;
            }

            let encoded_byte = buf[0];
            value += u32::from(encoded_byte & 127) * multiplier;
            multiplier *= 128;

            if encoded_byte & 128 == 0 {
                break;
            }

            assert!(
                multiplier <= THRESHOLD,
                "malformed remaining length {}",
                multiplier
            );
        }
        Some(value as usize)
    }
}

impl Stream for MQTTCodec {
    type Item = Packet;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("polling MQTT stream: {:?}", self.rd);
        // First, read any new data that might have been received off the socket
        let sock_closed = self.fill_read_buf()?.is_ready();

        if self.rd.len() > 0 {
            let first_packet = self.rd.split_to(1)[0];
            let packet_type = first_packet >> 4;
            let flags = first_packet & 0b0000_1111;
            let remaining_length = match self.take_variable_length() {
                None => return Ok(Async::NotReady),
                Some(i) => i,
            };

            if self.rd.len() < remaining_length {
                return Ok(Async::NotReady);
            }

            let payload = self.rd.split_to(remaining_length);

            info!(
                "Packet type: {:08b}; flags: {:8b}; remaining length: {:?}; payload: {:?}; buf len: {:?}",
                packet_type,
                flags,
                remaining_length,
                payload,
                self.rd.len()
            );

            //return parsed packet
            Ok(Async::Ready(Some(Packet {})))
        } else if sock_closed {
            //closed socket?
            Ok(Async::Ready(None))
        } else {
            //not enough
            Ok(Async::NotReady)
        }
    }
}

fn handle_new_client((connect, packets): (Option<Packet>, MQTTCodec)) -> Client {
    //match if ConnectPacket, create client
    info!("new client connected: {:?}", connect);
    Client::new("client_id".to_string(), packets)
}

fn handle_error(e: io::Error) {
    error!("connection error = {:?}", e);
}

fn process(socket: TcpStream, broker: Arc<Mutex<Broker>>) -> Box<Future<Item = (), Error = ()> + Send> {
    info!(
        "new connection accepted from: {:?} to broker: {:?}",
        socket.peer_addr(),
        broker
    );

    let msg = MQTTCodec::new(socket)
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(handle_new_client)
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
