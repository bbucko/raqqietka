use crate::MQTTError;

pub fn take_u18(bytes: &[u8]) -> Result<(u16, &[u8]), MQTTError> {
    if bytes.len() < 2 {
        return Err(format!("malformed take_u18: {}", bytes.len()).into());
    }
    let (length_bytes, bytes) = bytes.split_at(2);
    Ok(((u16::from(length_bytes[0]) << 8) + u16::from(length_bytes[1]), bytes))
}

pub fn take_string(bytes: &[u8]) -> Result<(String, &[u8]), MQTTError> {
    let (string_length, bytes) = take_u18(bytes).map_err(|_| "invalid string length")?;
    let (string_bytes, bytes) = bytes.split_at(string_length as usize);
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|e| format!("malformed: {}", e))?;
    Ok((string, bytes))
}

pub fn check_flag(flags: u8, position: usize) -> bool {
    (flags >> position) & 1u8 == 1u8
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
}
