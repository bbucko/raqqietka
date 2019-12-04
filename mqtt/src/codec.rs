use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use core::*;

use crate::num_traits::FromPrimitive;
use crate::PacketsCodec;
use bytes::buf::Buf;

impl PacketsCodec {
    pub fn new() -> PacketsCodec {
        PacketsCodec::default()
    }
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

        let (payload_length, header_length) = match decode_length(src, 1)? {
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
        let bytes: Bytes = item.into();
        dst.reserve(bytes.len());
        dst.put(bytes);
        Ok(())
    }
}

fn decode_length(buffer: &mut BytesMut, start: usize) -> Result<Option<(usize, usize)>, MQTTError> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut index = start;

    loop {
        if buffer.len() <= index {
            return Ok(None);
        };

        let encoded_byte = buffer[index];
        value += (encoded_byte & 127) as usize * multiplier;
        if multiplier > 128 * 128 * 128 {
            return Err("unknown_error".into());
        }
        multiplier *= 128;

        if encoded_byte & 128 == 0 {
            break;
        }
        index += 1;
    }
    Ok(Some((value, index)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_length_one_byte() {
        assert_eq!(Ok(Some((0, 0))), decode_length(&mut BytesMut::from(&vec![0x00][..]), 0));
        assert_eq!(Ok(Some((127, 0))), decode_length(&mut BytesMut::from(&vec![0x7f][..]), 0));
    }

    #[test]
    fn test_decode_length_two_bytes() {
        assert_eq!(Ok(Some((128, 1))), decode_length(&mut BytesMut::from(&vec![0x80, 0x01][..]), 0));
        assert_eq!(Ok(Some((16383, 1))), decode_length(&mut BytesMut::from(&vec![0xff, 0x7f][..]), 0));
    }

    #[test]
    fn test_decode_length_three_bytes() {
        assert_eq!(Ok(Some((16384, 2))), decode_length(&mut BytesMut::from(&vec![0x80, 0x80, 0x01][..]), 0));
        assert_eq!(Ok(Some((2097151, 2))), decode_length(&mut BytesMut::from(&vec![0xFF, 0xFF, 0x7F][..]), 0));
    }

    #[test]
    fn test_decode_length_four_bytes() {
        assert_eq!(Ok(Some((2097152, 3))), decode_length(&mut BytesMut::from(&vec![0x80, 0x80, 0x80, 0x01][..]), 0));
        assert_eq!(
            Ok(Some((268435455, 3))),
            decode_length(&mut BytesMut::from(&vec![0xFF, 0xFF, 0xFF, 0x7F][..]), 0)
        );
    }

    #[test]
    fn test_decode_not_enough_bytes() {
        assert_eq!(Ok(None), decode_length(&mut BytesMut::from(&vec![0][..]), 1));
    }
}
