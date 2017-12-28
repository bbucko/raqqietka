use std::io;
use std::string;


static THRESHOLD: u32 = 128 * 128 * 128;

pub trait MQTTRead {
    fn take_variable_length(&mut self) -> Result<usize, io::Error>;
    fn take_one_byte(&mut self) -> Result<u8, io::Error>;
}

impl<T> MQTTRead for T
    where
        T: io::Read,
{
    fn take_variable_length(&mut self) -> Result<usize, io::Error> {
        let mut multiplier: u32 = 1;
        let mut value: u32 = 0;
        let mut encoded_byte = [0];

        loop {
            let _ = self.read_exact(&mut encoded_byte)?;
            value += u32::from(encoded_byte[0] & 127) * multiplier;
            multiplier *= 128;

            if encoded_byte[0] & 128 == 0 {
                break;
            }

            assert!(multiplier <= THRESHOLD, "malformed remaining length {}", multiplier);
        }
        Ok(value as usize)
    }

    fn take_one_byte(&mut self) -> Result<u8, io::Error> {
        let mut buf = [0];
        let _ = self.read_exact(&mut buf)?;

        Ok(buf[0])
    }
}

pub trait MQTTVector {
    fn take_string(&mut self) -> string::String;
    fn take_length(&mut self) -> usize;
    fn take_one_byte(&mut self) -> u8;
    fn take_two_bytes(&mut self) -> u16;
    fn take_payload(&mut self) -> Vec<u8>;
}

impl MQTTVector for Vec<u8> {
    fn take_string(&mut self) -> string::String {
        let length = self.take_length();
        let proto_name: Vec<u8> = self.drain(0..length).collect();
        String::from_utf8(proto_name).expect("Error unwrapping string")
    }

    fn take_length(&mut self) -> usize {
        let length: Vec<u8> = self.drain(0..2).collect();
        ((length[0] as usize) << 8) | (length[1] as usize)
    }

    fn take_one_byte(&mut self) -> u8 {
        let take_flags: Vec<u8> = self.drain(0..1).collect();
        take_flags[0]
    }

    fn take_two_bytes(&mut self) -> u16 {
        let keep_alive: Vec<u8> = self.drain(0..2).collect();
        (u16::from(keep_alive[0]) << 8) | u16::from(keep_alive[1])
    }

    fn take_payload(&mut self) -> Vec<u8> {
        self.drain(0..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_one_byte_from_read() {
        assert_matches!(io::Cursor::new(vec![0b00000000]).take_one_byte(), Ok(0));
        assert_matches!(io::Cursor::new(vec![0x7F]).take_variable_length(), Ok(127));
    }

    #[test]
    fn test_take_variable_length() {
        assert_matches!(io::Cursor::new(vec![0b00000000]).take_variable_length(), Ok(0));
        assert_matches!(io::Cursor::new(vec![0x7F]).take_variable_length(), Ok(127));

        assert_matches!(io::Cursor::new(vec![0x80, 0x01]).take_variable_length(), Ok(128));
        assert_matches!(io::Cursor::new(vec![0xFF, 0x7F]).take_variable_length(), Ok(16_383));

        assert_matches!(io::Cursor::new(vec![0x80, 0x80, 0x01]).take_variable_length(), Ok(16_384));
        assert_matches!(io::Cursor::new(vec![0xFF, 0xFF, 0x7F]).take_variable_length(), Ok(2_097_151));

        assert_matches!(io::Cursor::new(vec![0x80, 0x80, 0x80, 0x01]).take_variable_length(), Ok(2_097_152));
        assert_matches!(io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0x7F]).take_variable_length(), Ok(268_435_455));
    }

    #[test]
    #[should_panic]
    fn test_take_variable_length_malformed() {
        let _ = io::Cursor::new(vec![0xFF, 0xFF, 0xFF, 0x8F]).take_variable_length();
    }

    #[test]
    fn test_take_two_bytes() {
        assert_eq!(vec![0, 0].take_two_bytes(), 0);
    }

    #[test]
    fn test_take_one_byte() {
        assert_eq!(vec![0].take_one_byte(), 0);
    }

    #[test]
    fn test_length() {
        assert_eq!(vec![0, 0].take_length(), 0);
        assert_eq!(vec![0, 1].take_length(), 1);
        assert_eq!(vec![1, 0].take_length(), 256);
        assert_eq!(vec![1, 1].take_length(), 257);
        assert_eq!(vec![1, 1, 1].take_length(), 257);
    }
}
