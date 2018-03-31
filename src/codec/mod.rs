use bytes::{BufMut, BytesMut};
use tokio::io;
use tokio::net::TcpStream;
use tokio::prelude::*;

static THRESHOLD: u32 = 128 * 128 * 128;

#[derive(Debug)]
pub struct Packet {
    pub packet_type: u8,
    pub flags: u8,
    pub payload: BytesMut,
}

impl Packet {
    pub fn new(header: u8, payload: BytesMut) -> Self {
        let packet_type = header >> 4;
        let flags = header & 0b0000_1111;
        Packet { packet_type, flags, payload }
    }
}

#[derive(Debug)]
pub struct MQTT {
    stream: TcpStream,
    rd: BytesMut,
    wr: BytesMut,
}

impl MQTT {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: stream,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    pub fn poll_flush(&mut self) -> Poll<(), io::Error> {
        while !self.wr.is_empty() {
            let n = try_ready!(self.stream.poll_write(&self.wr));

            assert!(n > 0);

            self.wr.advance(n);
        }

        Ok(Async::Ready(()))
    }

    pub fn buffer(&mut self, packet: &[u8]) {
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
                debug!("closing socket: {:?}", self.stream.peer_addr().unwrap());
                return Ok(Async::Ready(()));
            }
        }
    }

    fn read_variable_length(&mut self) -> usize {
        let mut multiplier = 1;
        let mut value: u32 = 0;
        let mut pos = 0;
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

    fn read_header(&mut self) -> u8 { self.rd.split_to(1)[0] }

    fn read_payload(&mut self) -> BytesMut {
        let payload_length = self.read_variable_length();
        self.rd.split_to(payload_length)
    }
}

impl Stream for MQTT {
    type Item = Packet;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        debug!("polling MQTT stream: {:?}", self.rd);
        // First, read any new data that might have been received off the socket
        let sock_closed = self.fill_read_buf()?.is_ready();

        if self.rd.len() > 1 {
            //return parsed packet
            Ok(Async::Ready(Some(Packet::new(self.read_header(), self.read_payload()))))
        } else if sock_closed {
            //closed socket?
            debug!("closed socket: {:?}", self.stream.peer_addr().unwrap());
            Ok(Async::Ready(None))
        } else {
            //not enough
            Ok(Async::NotReady)
        }
    }
}
