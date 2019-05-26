use std::io;

use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::prelude::*;

use mqtt::Packets;
use {MQTTError, Packet};

impl From<io::Error> for MQTTError {
    fn from(_: io::Error) -> Self { unimplemented!() }
}

impl Packets {
    pub fn new(socket: TcpStream) -> Packets {
        Packets {
            socket,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    pub fn buffer(&mut self, packet: Packet) {
        let bytes: Bytes = packet.into();
        self.wr.extend(bytes);
    }

    fn fill_read(&mut self) -> Result<Async<()>, MQTTError> {
        loop {
            self.rd.reserve(1024);
            let read = self.socket.read_buf(&mut self.rd);
            let bytes_read = try_ready!(read);
            if bytes_read == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }

    pub fn poll_flush(&mut self) -> Poll<(), MQTTError> {
        while !self.wr.is_empty() {
            let write = self.socket.poll_write(&self.wr);
            let bytes_written = try_ready!(write);
            assert!(bytes_written > 0);
            self.wr.advance(bytes_written);
        }
        Ok(Async::Ready(()))
    }
}

impl Stream for Packets {
    type Item = Packet;
    type Error = MQTTError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let sock_closed = self.fill_read()?.is_ready();

        Packet::from(&mut self.rd).map(|packet| {
            if packet.is_some() {
                Async::Ready(packet)
            } else if sock_closed {
                //socket is closed. should disconnect.
                Async::Ready(None)
            } else {
                //waiting for more bytes
                Async::NotReady
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_packet() {
        let mut raw_packet = BytesMut::from(&b""[..]);
        let parsed_packet = Packet::from(&mut raw_packet).unwrap();
        assert!(parsed_packet.is_none());
    }

    #[test]
    fn test_missing_payload() {
        let mut raw_packet = BytesMut::from(&b"\x10>"[..]);
        let parsed_packet = Packet::from(&mut raw_packet).unwrap();
        assert!(parsed_packet.is_none());
    }

    #[test]
    fn test_parse_sample_connect() {
        let mut raw_packet = BytesMut::from(&b"\x10>\0\x04MQTT\x04\xc6\0<\0\x03abc\0\x0b/will/topic\0\x0cwill message\0\x08username\0\x08password"[..]);
        let parsed_packet = Packet::from(&mut raw_packet).unwrap();
        assert!(parsed_packet.is_some());
    }
}
