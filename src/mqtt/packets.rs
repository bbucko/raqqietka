use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use num_traits::{FromPrimitive, ToPrimitive};
use tokio::net::TcpStream;
use tokio::prelude::*;

use mqtt::Packets;
use mqtt::PacketType;
use Packet;

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Packet: ({:?}, {:#010b}, {:?})", self.packet_type, self.flags, self.payload)
    }
}

impl Packet {
    pub fn from(buffer: &mut BytesMut) -> Result<Option<Packet>, io::Error> {
        if buffer.is_empty() {
            return Ok(None);
        }

        let control_and_flags = buffer[0];
        if control_and_flags == 0 {
            return Ok(None);
        }

        let packet_type = PacketType::from_u8(control_and_flags >> 4).expect("unknown packet type");
        let flags = control_and_flags & 0b0000_1111;

        let (payload_length, header_length) = match Packet::decode_length(buffer, 1)? {
            Some((payload_length, read_bytes)) => (payload_length, read_bytes + 1),
            None => {
                return Ok(None);
            }
        };

        if buffer.len() < header_length + payload_length {
            return Ok(None);
        }

        buffer.advance(header_length);

        let payload = buffer.split_to(payload_length).freeze();
        Ok(Some(Packet { packet_type, flags, payload: Some(payload) }))
    }

    fn decode_length(buffer: &mut BytesMut, start: usize) -> Result<Option<(usize, usize)>, io::Error> {
        let mut multiplier = 1;
        let mut value = 0;
        let mut index = start;

        loop {
            if buffer.len() < index {
                return Ok(None);
            };

            let encoded_byte = buffer[index];
            value += (encoded_byte & 127) as usize * multiplier;
            if multiplier > 128 * 128 * 128 {
                return Err(io::Error::new(io::ErrorKind::Other, "foo"));
            }
            multiplier *= 128;

            if encoded_byte & 128 == 0 {
                break;
            }
            index += 1;
        }
        Ok(Some((value, index)))
    }

    fn encode_length(length: usize) -> Bytes {
        let mut bytes = BytesMut::new();
        let mut x = length;
        let mut encoded_byte;
        loop {
            encoded_byte = x % 128;
            x /= 128;

            if x > 0 {
                encoded_byte |= 128;
            }

            bytes.put_u8(encoded_byte as u8);

            if x == 0 { break; }
        }
        bytes.freeze()
    }

    fn encode_string(string: String) -> Bytes {
        let mut encoded_string = BytesMut::new();
        encoded_string.put_u16_be(string.len() as u16);
        encoded_string.put(string);

        encoded_string.freeze()
    }

    fn type_and_flags(packet_type: &PacketType, flags: u8) -> u8 {
        assert!(flags <= 0b0000_1111);

        packet_type.to_u8()
            .map(|packet_type| (packet_type << 4) + flags)
            .unwrap()
    }

    pub fn publish(topic: String, payload: Bytes, qos: u8) -> Packet {
        let mut packet = BytesMut::new();
        packet.put(Packet::encode_string(topic));
        packet.put_u16_be(1);
        packet.put(payload);

        assert!(qos < 3);
        let flags = qos << 1;

        Packet {
            packet_type: PacketType::PUBLISH,
            flags,
            payload: Some(packet.freeze()),
        }
    }

    pub fn puback(packet_identifier: u16) -> Packet {
        info!("Responded with PUBACK for packet id: {}", packet_identifier);
        let mut payload = BytesMut::new();
        payload.put_u16_be(packet_identifier);

        Packet {
            packet_type: PacketType::PUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }

    pub fn suback(packet_identifier: u16) -> Packet {
        let mut payload = BytesMut::new();
        payload.put_u16_be(packet_identifier);


        info!("Responded with SUBACK");

        Packet {
            packet_type: PacketType::SUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }

    pub fn connack() -> Packet {
        let mut payload = BytesMut::new();
        payload.put_u8(0b0000_0000);
        payload.put_u8(0b0000_0000);

        info!("Responded with CONNACK");

        Packet {
            packet_type: PacketType::CONNACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }

    pub fn pingres() -> Packet {
        info!("Responded with PINGRES");

        Packet {
            packet_type: PacketType::PINGRES,
            flags: 0,
            payload: None,
        }
    }
}

impl Into<Bytes> for Packet {
    fn into(self) -> Bytes {
        let packet_type = &self.packet_type;
        let flags = self.flags;

        let payload = self.payload.map_or_else(Bytes::new, Bytes::into);
        let packet_length = payload.len();

        let encoded_packet_length = Packet::encode_length(packet_length);

        let mut bytes = BytesMut::with_capacity(1 + encoded_packet_length.len() + packet_length);
        bytes.put_u8(Packet::type_and_flags(packet_type, flags));
        bytes.put(encoded_packet_length);
        bytes.put(payload);
        bytes.freeze()
    }
}

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

    pub fn poll_flush(&mut self) -> Poll<(), io::Error> {
        while !self.wr.is_empty() {
            let n = try_ready!(self.socket.poll_write(&self.wr));
            assert!(n > 0);
            self.wr.advance(n);
        }
        Ok(Async::Ready(()))
    }

    fn transform(packet: Option<Packet>, sock_closed: bool) -> Async<Option<Packet>> {
        packet.map_or_else(
            || if sock_closed { Async::Ready(None) } else { Async::NotReady },
            |packet| Async::Ready(Some(packet)),
        )
    }
}

impl Stream for Packets {
    type Item = Packet;
    type Error = io::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        let sock_closed = self.fill_read()?.is_ready();

        Packet::from(&mut self.rd)
            .map(|packet| Packets::transform(packet, sock_closed))
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


    #[test]
    fn test_connect() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b0000_0000);
        assert_eq!(header, 0b0001_0000);
    }

    #[test]
    fn test_connect_with_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b0000_0001);
        assert_eq!(header, 0b0001_0001);
    }

    #[test]
    #[should_panic]
    fn test_connect_with_invalid_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b1000_0001);
        assert_eq!(header, 0b0001_0001);
    }

    #[test]
    fn test_connack_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNACK, 0b0000_0000);
        assert_eq!(header, 0b0010_0000);
    }

    #[test]
    fn test_publish() {
        let header = Packet::type_and_flags(&PacketType::PUBLISH, 0b0000_1111);
        assert_eq!(header, 0b0011_1111);
    }

    #[test]
    fn test_encoding_one_digit() {
        assert_eq!(Bytes::from(&[0x00u8][..]), Packet::encode_length(0));
        assert_eq!(Bytes::from(&[0x7Fu8][..]), Packet::encode_length(127));
    }

    #[test]
    fn test_encoding_two_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x01u8][..]), Packet::encode_length(128));
        assert_eq!(Bytes::from(&[0xFFu8, 0x7Fu8][..]), Packet::encode_length(16_383));
    }

    #[test]
    fn test_encoding_three_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x80u8, 0x01u8][..]), Packet::encode_length(16_384));
        assert_eq!(Bytes::from(&[0xFFu8, 0xFFu8, 0x7Fu8][..]), Packet::encode_length(2_097_151));
    }

    #[test]
    fn test_encoding_four_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x80u8, 0x80u8, 0x01u8][..]), Packet::encode_length(2_097_152));
        assert_eq!(Bytes::from(&[0xFFu8, 0xFFu8, 0xFFu8, 0x7Fu8][..]), Packet::encode_length(268_435_455));
    }

    #[test]
    fn test_connack() {
        assert_eq!(Bytes::from(&[0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000][..]), Into::<Bytes>::into(Packet::connack()));
    }
}
