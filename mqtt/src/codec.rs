use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use core::*;

use crate::PacketsCodec;

impl PacketsCodec {
    pub fn new() -> PacketsCodec {
        PacketsCodec::default()
    }
}

impl Decoder for PacketsCodec {
    type Item = mqttrs::Packet;
    type Error = MQTTError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        mqttrs::decode(src).map_err(|_| MQTTError::ClientError("error".to_string()))
    }
}

impl Encoder<mqttrs::Packet> for PacketsCodec {
    type Error = MQTTError;

    fn encode(&mut self, item: mqttrs::Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        mqttrs::encode(&item, dst).map_err(|_| MQTTError::ClientError("error".to_string()))
    }
}
