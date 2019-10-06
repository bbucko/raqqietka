use std::convert::TryFrom;

use bytes::{BufMut, BytesMut};

use broker::{util, MQTTError, Packet, PacketType};

use crate::{ConnAck, PingResp, PubAck, SubAck, UnsubAck};

impl From<PubAck> for Packet {
    fn from(puback: PubAck) -> Self {
        info!("Responded with PUBACK for packet id: {}", puback.packet_id);

        let mut payload = BytesMut::with_capacity(2);
        payload.put_u16_be(puback.packet_id);

        Packet {
            packet_type: PacketType::PUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl From<SubAck> for Packet {
    fn from(suback: SubAck) -> Self {
        let mut payload = BytesMut::with_capacity(2 + suback.sub_results.len());
        payload.put_u16_be(suback.packet_id);
        payload.extend(suback.sub_results);

        info!("Responded with SUBACK: {:?}", payload);

        Packet {
            packet_type: PacketType::SUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl From<UnsubAck> for Packet {
    fn from(unsuback: UnsubAck) -> Self {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u16_be(unsuback.packet_id);

        info!("Responded with UNSUBACK: {:?}", payload);

        Packet {
            packet_type: PacketType::UNSUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl From<ConnAck> for Packet {
    fn from(_: ConnAck) -> Self {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u8(0b0000_0000);
        payload.put_u8(0b0000_0000);

        info!("Responded with CONNACK");

        Packet {
            packet_type: PacketType::CONNACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl From<PingResp> for Packet {
    fn from(_: PingResp) -> Self {
        Packet {
            packet_type: PacketType::PINGRES,
            flags: 0,
            payload: None,
        }
    }
}

impl TryFrom<Packet> for PubAck {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::PUBACK, packet.packet_type);
        let payload = packet.payload.ok_or("malformed")?;
        let (packet_id, payload) = util::take_u18(&payload)?;

        assert!(payload.is_empty());

        Ok(PubAck { packet_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnAck;
    use bytes::Bytes;

    #[test]
    fn test_connack() {
        let conn_ack: Packet = ConnAck {}.into();
        assert_eq!(
            Bytes::from(&[0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000][..]),
            Into::<Bytes>::into(conn_ack)
        );
    }
}
