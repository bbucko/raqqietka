use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::sync::PoisonError;
use std::{error, fmt};

use crate::MQTTError::OtherError;
use crate::{Broker, MQTTError};

impl error::Error for MQTTError {}

impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, Broker>>> for MQTTError {
    fn from(err: PoisonError<std::sync::MutexGuard<'_, Broker>>) -> Self { MQTTError::ServerError(err.to_string()) }
}

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
