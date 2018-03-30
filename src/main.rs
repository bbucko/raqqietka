#![deny(warnings)]

extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate tokio;

use bytes::{BufMut, Bytes, BytesMut};

use futures::future::{self, Either};
use futures::sync::mpsc;
use futures::{Future, Stream};

use log::LogLevel;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

static THRESHOLD: u32 = 128 * 128 * 128;
static LINES_PER_TICK: usize = 10;

type Tx = mpsc::UnboundedSender<Bytes>;
type Rx = mpsc::UnboundedReceiver<Bytes>;

#[derive(Debug)]
pub struct Client {
    packets: MQTTCodec,
    client_info: String,
    rx: Rx,
    tx: Tx,
}

impl Future for Client {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Poll<(), io::Error> {
        debug!("flush outbound queue: {:?}", self.rx);
        for i in 0..LINES_PER_TICK {
            match self.rx.poll().unwrap() {
                Async::Ready(Some(v)) => {
                    info!("sending packets: {:?}", v);
                    self.packets.buffer(&v);

                    //be good to others my dear future ;)
                    if i + 1 == LINES_PER_TICK {
                        task::current().notify();
                    }
                }
                _ => break,
            }
        }

        self.packets.poll_flush()?;

        while let Async::Ready(packet) = self.packets.poll()? {
            if let Some(packet) = packet {
                //handle various packets
                info!("Received parsed packet ({:?}) : {:?} for client: {:?}", self, packet, self.client_info);

                match packet.packet_type {
                    8 => info!("Subscribe: {:?}", packet.payload),
                    12 => {
                        info!("Ping RQ: {:?}", packet);
                        let ping_resp_buf = Bytes::from(vec![0b1101_0000, 0b0000_0000]);
                        self.tx.unbounded_send(ping_resp_buf).unwrap();
                    }
                    _ => {
                        error!("unknown packet: {:?}", packet.packet_type);
                        panic!("unknown packet")
                    }
                };
            } else {
                //abort, abort, abort
                return Ok(Async::Ready(()));
            }
        }
        //verify if packets returned NotReady
        Ok(Async::NotReady)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        info!("Disconecting: {:?}", self);
    }
}

impl Client {
    pub fn new(client_info: String, packets: MQTTCodec) -> Self {
        let (tx, rx) = mpsc::unbounded();

        tx.unbounded_send(Bytes::from(vec![0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000]))
            .unwrap();

        Client {
            client_info: client_info,
            packets: packets,
            rx: rx,
            tx: tx,
        }
    }
}

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

    fn take_byte(&mut self) -> u8 {
        self.payload.split_to(1)[0]
    }

    fn take_string(&mut self) -> Bytes {
        let length_buf = self.payload.split_to(2);
        let length = (usize::from(length_buf[0]) << 8) | usize::from(length_buf[1]);
        if length > self.payload.len() {
            error!("provided len ({:?}) greater then packet len: {:?}", length, self.payload.len());
            panic!("invalid packet format");
        }
        self.payload.split_to(length).freeze()
    }
}

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

    fn poll_flush(&mut self) -> Poll<(), io::Error> {
        while !self.wr.is_empty() {
            let n = try_ready!(self.stream.poll_write(&self.wr));

            assert!(n > 0);

            self.wr.advance(n);
        }

        Ok(Async::Ready(()))
    }

    fn buffer(&mut self, packet: &[u8]) {
        self.wr.reserve(packet.len());
        self.wr.put(packet);
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

    fn read_variable_length(&mut self) -> usize {
        let mut multiplier = 1;
        let mut value: u32 = 0;
        let mut pos: usize = 0;
        loop {
            let encoded_byte = self.rd[pos];
            value += u32::from(encoded_byte & 127) * multiplier;
            multiplier *= 128;
            pos = pos + 1;

            if encoded_byte & 128 == 0 {
                break;
            }

            assert!(multiplier <= THRESHOLD, "malformed remaining length {}", multiplier);
        }
        self.rd.advance(pos);
        value as usize
    }

    fn read_header(&mut self) -> u8 {
        let first_packet = self.rd[0];
        self.rd.advance(1);
        first_packet
    }

    fn read_payload(&mut self) -> BytesMut {
        let payload_length = self.read_variable_length();
        self.rd.split_to(payload_length)
    }
}

impl Stream for MQTTCodec {
    type Item = Packet;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("polling MQTT stream: {:?}", self.rd);
        // First, read any new data that might have been received off the socket
        let sock_closed = self.fill_read_buf()?.is_ready();

        if self.rd.len() > 1 {
            let header = self.read_header();
            let payload = self.read_payload();

            //return parsed packet
            Ok(Async::Ready(Some(Packet::new(header, payload))))
        } else if sock_closed {
            //closed socket?
            Ok(Async::Ready(None))
        } else {
            //not enough
            Ok(Async::NotReady)
        }
    }
}

fn parse_connect_packet(mut connect: Packet, packets: MQTTCodec) -> Client {
    info!("Connect: {:?}", connect);

    if connect.packet_type != 1 {
        panic!("invalid packet type");
    }

    let proto_name = connect.take_string();
    if proto_name != "MQTT" {
        panic!("invalid proto")
    }

    let proto_level = connect.take_byte();
    if proto_level != 4 {
        panic!("invalid proto level")
    }

    Client::new("client_id".to_string(), packets)
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
                Some(connect) => Either::A(parse_connect_packet(connect, packets)),
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
