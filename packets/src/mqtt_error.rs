use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::{error, fmt};

use crate::MQTTError;
use crate::MQTTError::OtherError;

impl error::Error for MQTTError {}

impl From<&str> for MQTTError {
    fn from(str: &str) -> Self { MQTTError::OtherError(String::from(str)) }
}

impl From<std::io::Error> for MQTTError {
    fn from(str: std::io::Error) -> Self { MQTTError::OtherError(str.to_string()) }
}

impl From<MQTTError> for std::io::Error {
    fn from(src: MQTTError) -> Self { std::io::Error::new(ErrorKind::Other, src.to_string()) }
}

impl From<String> for MQTTError {
    fn from(str: String) -> Self { MQTTError::OtherError(str) }
}

impl Display for MQTTError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            OtherError(ref err) => write!(f, "{}", err),
            _ => write!(f, "Error"),
        }
    }
}
