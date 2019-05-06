use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::prelude::*;

use mqtt::Packets;
use {MQTTError, Packet};

impl Packets {
    pub fn new(socket: TcpStream) -> Packets {
        Packets {
            socket,
            rd: BytesMut::new(),
            wr: BytesMut::new(),
        }
    }

    fn fill_read(&mut self) -> Result<Async<()>, io::Error> {
        loop {
            self.rd.reserve(1024);
            if try_ready!(self.socket.read_buf(&mut self.rd)) == 0 {
                return Ok(Async::Ready(()));
            }
        }
    }

    pub fn buffer(&mut self, packet: Packet) {
        let bytes: Bytes = packet.into();
        self.wr.reserve(bytes.len());
        self.wr.put(bytes);
    }

    pub fn poll_flush(&mut self) -> Poll<(), MQTTError> {
        while !self.wr.is_empty() {
            let n = try_ready!(self.socket.poll_write(&self.wr).map_err(|err| format!("{}", err)));
            assert!(n > 0);
            self.wr.advance(n);
        }
        Ok(Async::Ready(()))
    }

    fn handle_socket(packet: Option<Packet>, sock_closed: bool) -> Async<Option<Packet>> {
        packet.map_or_else(
            || if sock_closed { Async::Ready(None) } else { Async::NotReady },
            |packet| Async::Ready(Some(packet)),
        )
    }
}

impl Stream for Packets {
    type Item = Packet;
    type Error = MQTTError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let sock_closed = self.fill_read().map_err(|err| format!("unknown error: {}", err))?.is_ready();

        Packet::from(&mut self.rd)
            .map(|packet| Packets::handle_socket(packet, sock_closed))
            .map_err(|err| format!("unknown error: {}", err))
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
