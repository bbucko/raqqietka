use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

use MQTTError;

pub fn decode_length(buffer: &mut BytesMut, start: usize) -> Result<Option<(usize, usize)>, MQTTError> {
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
            return Err(format!("foo").into());
        }
        multiplier *= 128;

        if encoded_byte & 128 == 0 {
            break;
        }
        index += 1;
    }
    Ok(Some((value, index)))
}

pub fn encode_length(length: usize) -> Bytes {
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

        if x == 0 {
            break;
        }
    }
    bytes.freeze()
}

pub fn encode_string(string: String) -> Bytes {
    let mut encoded_string = BytesMut::new();
    encoded_string.put_u16_be(string.len() as u16);
    encoded_string.put(string);

    encoded_string.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_one_digit() {
        assert_eq!(Bytes::from(&[0x00u8][..]), encode_length(0));
        assert_eq!(Bytes::from(&[0x7Fu8][..]), encode_length(127));
    }

    #[test]
    fn test_encoding_two_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x01u8][..]), encode_length(128));
        assert_eq!(Bytes::from(&[0xFFu8, 0x7Fu8][..]), encode_length(16_383));
    }

    #[test]
    fn test_encoding_three_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x80u8, 0x01u8][..]), encode_length(16_384));
        assert_eq!(Bytes::from(&[0xFFu8, 0xFFu8, 0x7Fu8][..]), encode_length(2_097_151));
    }

    #[test]
    fn test_encoding_four_digits() {
        assert_eq!(Bytes::from(&[0x80u8, 0x80u8, 0x80u8, 0x01u8][..]), encode_length(2_097_152));
        assert_eq!(Bytes::from(&[0xFFu8, 0xFFu8, 0xFFu8, 0x7Fu8][..]), encode_length(268_435_455));
    }
}
