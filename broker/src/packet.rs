use std::fmt;
use std::fmt::Error;
use std::fmt::Formatter;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use num_traits::ToPrimitive;

use crate::{util, Packet, PacketType, Publish};

impl From<Publish> for Packet {
    fn from(publish: Publish) -> Self {
        info!("Sending PUBLISH for packet id: {}", publish.packet_id);

        let payload = publish.payload;
        let topic = util::encode_string(publish.topic);

        let mut packet = BytesMut::with_capacity(topic.len() + 2 + payload.len());
        packet.put(topic);
        packet.put_u16_be(publish.packet_id);
        packet.put(payload);

        let flags = publish.qos << 1;

        Packet {
            packet_type: PacketType::PUBLISH,
            flags,
            payload: Some(packet.freeze()),
        }
    }
}

impl Packet {
    fn type_and_flags(packet_type: &PacketType, flags: u8) -> u8 {
        assert!(flags <= 0b0000_1111);
        packet_type.to_u8().map(|packet_type| (packet_type << 4) + flags).unwrap()
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

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> { write!(f, "Packet: ({:?}, {:#010b}, {:?})", self.packet_type, self.flags, self.payload) }
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
}
