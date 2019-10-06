use bytes::{BufMut, Bytes, BytesMut};
use tokio::codec::{Decoder, Encoder};

use broker::{MQTTError, Packet, PacketType};

use crate::num_traits::FromPrimitive;
use crate::{util, PacketsCodec};

impl PacketsCodec {
    pub fn new() -> PacketsCodec { PacketsCodec {} }
}

impl Decoder for PacketsCodec {
    type Item = Packet;
    type Error = MQTTError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        //ensure that enough bytes were transmitted
        if src.is_empty() {
            return Ok(None);
        }

        let control_and_flags = src.first().unwrap();
        let packet_type = PacketType::from_u8(control_and_flags >> 4).unwrap();
        let flags = control_and_flags & 0b0000_1111;

        let (payload_length, header_length) = match util::decode_length(src, 1)? {
            Some((payload_length, read_bytes)) => (payload_length, read_bytes + 1),
            None => {
                return Ok(None);
            }
        };

        if src.len() < header_length + payload_length {
            return Ok(None);
        }

        src.advance(header_length);

        let payload = src.split_to(payload_length).freeze();
        Ok(Some(Packet {
            packet_type,
            flags,
            payload: Some(payload),
        }))
    }
}

impl Encoder for PacketsCodec {
    type Item = Packet;
    type Error = MQTTError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dbg!(&item);

        let bytes: Bytes = item.into();
        dst.reserve(bytes.len());
        dst.put(bytes);
        return Ok(());
    }
}
