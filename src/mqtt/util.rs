use std::io;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

pub fn check_flag(flags: &u8, position: usize) -> bool {
    (flags >> position) & 1u8 == 1u8
}

pub fn decode_length(buffer: &mut BytesMut, start: usize) -> Result<Option<(usize, usize)>, io::Error> {
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

pub fn take_u18(bytes: &[u8]) -> Result<(u16, &[u8]), io::Error> {
    if bytes.len() < 2 {
        return Err(io::Error::new(io::ErrorKind::Other, "malformed"));
    }
    let (length_bytes, bytes) = bytes.split_at(2);
    Ok(((u16::from(length_bytes[0]) << 8) + u16::from(length_bytes[1]), bytes))
}

pub fn take_string(bytes: &[u8]) -> Result<(String, &[u8]), io::Error> {
    let (string_length, bytes) = take_u18(bytes).map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid string length"))?;
    let (string_bytes, bytes) = bytes.split_at(string_length as usize);
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok((string, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn test_calculate_length_short() {
        take_u18(&[0u8]).unwrap();
    }

    #[test]
    fn test_calculate_length() {
        assert_eq!(0, take_u18(&[0u8, 0u8]).unwrap().0);
        assert_eq!(4, take_u18(&[0u8, 0b000_0100]).unwrap().0);
        assert_eq!(257, take_u18(&[1u8, 1u8]).unwrap().0);
        assert_eq!(511, take_u18(&[1u8, 255u8]).unwrap().0);
        assert_eq!(65535, take_u18(&[255u8, 255u8]).unwrap().0);
    }

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
