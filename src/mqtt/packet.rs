use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;
use std::io;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use num_traits::FromPrimitive;
use num_traits::ToPrimitive;

use mqtt::util;
use mqtt::*;

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

        let (payload_length, header_length) = match util::decode_length(buffer, 1)? {
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
        Ok(Some(Packet {
            packet_type,
            flags,
            payload: Some(payload),
        }))
    }

    pub fn publish(packet_identifier: u16, topic: String, payload: Bytes, qos: u8) -> Packet {
        let mut packet = BytesMut::new();
        packet.put(util::encode_string(topic));
        packet.put_u16_be(packet_identifier);
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

    pub fn suback(packet_identifier: u16, subscription_results: &[u8]) -> Packet {
        let mut payload = BytesMut::new();
        payload.put_u16_be(packet_identifier);
        payload.put(subscription_results);

        info!("Responded with SUBACK: {:?}", payload);

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

    fn type_and_flags(packet_type: &PacketType, flags: u8) -> u8 {
        assert!(flags <= 0b0000_1111);
        packet_type.to_u8().map(|packet_type| (packet_type << 4) + flags).unwrap()
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Packet: ({:?}, {:#010b}, {:?})", self.packet_type, self.flags, self.payload)
    }
}

impl Into<Bytes> for Packet {
    fn into(self) -> Bytes {
        let packet_type = &self.packet_type;
        let flags = self.flags;

        let payload = self.payload.map_or_else(Bytes::new, Bytes::into);
        let packet_length = payload.len();

        let encoded_packet_length = util::encode_length(packet_length);

        let mut bytes = BytesMut::with_capacity(1 + encoded_packet_length.len() + packet_length);
        bytes.put_u8(Packet::type_and_flags(packet_type, flags));
        bytes.put(encoded_packet_length);
        bytes.put(payload);
        bytes.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_connack() {
        assert_eq!(
            Bytes::from(&[0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000][..]),
            Into::<Bytes>::into(Packet::connack())
        );
    }
}