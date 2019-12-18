#![warn(rust_2018_idioms)]

#[macro_use]
extern crate enum_primitive_derive;

use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::result;

use bytes::{BufMut, Bytes, BytesMut};
use num_traits::ToPrimitive;
use std::hash::{Hash, Hasher};
use tracing::debug;

mod mqtt_error;
mod util;

pub type ClientId = String;
pub type PacketId = u64;
pub type Topic = String;
pub type Qos = u8;

pub type MQTTResult<T> = result::Result<T, MQTTError>;

pub trait Publisher: Send + Debug + Display {
    fn send(&self, packet: Packet) -> MQTTResult<()>;
}

#[derive(Debug, PartialEq)]
pub enum MQTTError {
    ClientError(String),
    ServerError(String),
    OtherError(String),
}

//Should be in mqtt
#[derive(Debug, Clone, PartialEq)]
pub struct Packet {
    pub packet_type: PacketType,
    pub flags: u8,
    pub payload: Option<Bytes>,
}

//Should be in mqtt
#[derive(Debug, Primitive, PartialEq, Clone)]
pub enum PacketType {
    CONNECT = 1,
    CONNACK = 2,
    PUBLISH = 3,
    PUBACK = 4,
    //    PUBREC = 5,
    //    PUBREL = 6,
    //    PUBCOMP = 7,
    SUBSCRIBE = 8,
    SUBACK = 9,
    UNSUBSCRIBE = 10,
    UNSUBACK = 11,
    PINGREQ = 12,
    PINGRES = 13,
    DISCONNECT = 14,
}

#[derive(Debug)]
pub struct Connect {
    pub version: u8,
    pub client_id: Option<ClientId>,
    pub auth: Option<ConnectAuth>,
    pub will: Option<Will>,
    pub clean_session: bool,
}

#[derive(Debug)]
pub struct ConnectAuth {
    pub username: String,
    pub password: Option<Bytes>,
}

#[derive(Debug)]
pub struct Will {
    pub qos: Qos,
    pub retain: bool,
    pub topic: Topic,
    pub message: Bytes,
}

#[derive(Debug, Default)]
pub struct ConnAck {}

#[derive(Debug)]
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: Vec<(Topic, Qos)>,
}

#[derive(Debug)]
pub struct SubAck {
    pub packet_id: u16,
    pub sub_results: Vec<Qos>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub topics: Vec<Topic>,
}

#[derive(Debug)]
pub struct UnsubAck {
    pub packet_id: u16,
}

#[derive(Debug)]
pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct PubAck {
    pub packet_id: u16,
}

#[derive(Debug, Default)]
pub struct PingResp {}

#[derive(Debug, Default)]
pub struct Disconnect {}

impl Packet {
    fn type_and_flags(packet_type: &PacketType, flags: u8) -> u8 {
        assert!(flags <= 0b0000_1111);
        packet_type.to_u8().map(|packet_type| (packet_type << 4) + flags).unwrap()
    }
}

impl From<Packet> for Bytes {
    fn from(packet: Packet) -> Self {
        let packet_type = packet.packet_type;
        let flags = packet.flags;

        let payload = packet.payload.map_or_else(Bytes::new, Bytes::into);
        let packet_length = payload.len();

        let encoded_packet_length = util::encode_length(packet_length);

        let mut bytes = BytesMut::with_capacity(1 + encoded_packet_length.len() + packet_length);
        bytes.put_u8(Packet::type_and_flags(&packet_type, flags));
        bytes.put(encoded_packet_length);
        bytes.put(payload);
        bytes.freeze()
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ type = {:?}, flags = {:#010b}, payload.len = {} }}",
            self.packet_type,
            self.flags,
            self.payload.as_ref().map_or(0, |payload| payload.len())
        )
    }
}

impl TryFrom<Packet> for Connect {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::CONNECT, packet.packet_type);

        let payload = packet.payload.ok_or("malformed payload")?;

        let (proto_name, payload) = util::take_string(&payload)?;
        assert_eq!(proto_name.as_str(), "MQTT");

        let (&version, payload) = payload.split_first().ok_or("malformed version")?;
        let (&flags, payload) = payload.split_first().ok_or("malformed flags")?;

        let clean_session = util::check_flag(flags, 1);
        let (_keep_alive, payload) = util::take_u18(&payload)?;

        let (client_id, mut payload) = util::take_string(&payload)?;
        let will_present = util::check_flag(flags, 2);

        let will = if will_present {
            let retain = util::check_flag(flags, 5);
            let qos = util::take_qos_from_flags(flags);

            let internal_payload = payload;
            let (topic, internal_payload) = util::take_string(&internal_payload)?;
            let (length, internal_payload) = util::take_u18(&internal_payload)?;
            let (will_payload, internal_payload) = internal_payload.split_at(length as usize);

            //FIXME
            let message = Bytes::copy_from_slice(will_payload);
            let will_result = Will { qos, retain, topic, message };

            payload = internal_payload;

            Some(will_result)
        } else {
            if util::check_flag(flags, 5) || util::take_qos_from_flags(flags) != 0 {
                return Err(format!("malformed will").into());
            }

            None
        };

        let username_present = util::check_flag(flags, 7);

        let auth = if username_present {
            let internal_payload = payload;
            let (username, internal_payload) = util::take_string(&internal_payload)?;

            let password_present = util::check_flag(flags, 6);

            let password = if password_present {
                let (password_length, internal_payload) = util::take_u18(&internal_payload)?;
                let (password, internal_payload) = internal_payload.split_at(password_length as usize);

                payload = internal_payload;

                //FIXME
                Some(Bytes::copy_from_slice(password))
            } else {
                payload = internal_payload;

                None
            };

            Some(ConnectAuth { username, password })
        } else {
            None
        };

        assert!(payload.is_empty());

        Ok(Connect {
            version,
            client_id: Some(client_id),
            auth,
            will,
            clean_session,
        })
    }
}

impl From<ConnAck> for Packet {
    fn from(_: ConnAck) -> Self {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u8(0b0000_0000);
        payload.put_u8(0b0000_0000);

        debug!("creating CONNACK");

        Packet {
            packet_type: PacketType::CONNACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl TryFrom<Packet> for Subscribe {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::SUBSCRIBE, packet.packet_type);

        let payload = packet.payload.ok_or("malformed")?;
        let (packet_id, mut payload) = util::take_u18(&payload)?;

        let mut topics = Vec::new();

        loop {
            if payload.is_empty() {
                break;
            }

            let topic_payload = payload;

            let (topic, topic_payload) = util::take_string(&topic_payload)?;

            let (&qos, topic_payload) = topic_payload.split_first().ok_or("malformed")?;

            topics.push((topic, qos));
            payload = topic_payload;
        }

        Ok(Subscribe { packet_id, topics })
    }
}

impl From<SubAck> for Packet {
    fn from(suback: SubAck) -> Self {
        let mut payload = BytesMut::with_capacity(2 + suback.sub_results.len());
        payload.put_u16(suback.packet_id);
        payload.extend(suback.sub_results);

        debug!("creating SUBACK: {:?}", payload);

        Packet {
            packet_type: PacketType::SUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl TryFrom<Packet> for Unsubscribe {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::UNSUBSCRIBE, packet.packet_type);

        let payload = packet.payload.ok_or("malformed")?;
        let (packet_id, mut payload) = util::take_u18(&payload)?;

        let mut topics = Vec::new();
        loop {
            if payload.is_empty() {
                break;
            }

            let topic_payload = payload;

            let (topic, topic_payload) = util::take_string(&topic_payload)?;

            topics.push(topic);
            payload = topic_payload;
        }

        Ok(Unsubscribe { packet_id, topics })
    }
}

impl From<UnsubAck> for Packet {
    fn from(unsuback: UnsubAck) -> Self {
        let mut payload = BytesMut::with_capacity(2);
        payload.put_u16(unsuback.packet_id);

        debug!("creating UNSUBACK: {:?}", payload);

        Packet {
            packet_type: PacketType::UNSUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
        }
    }
}

impl From<Publish> for Packet {
    fn from(publish: Publish) -> Self {
        debug!("creating PUBLISH for packet id: {}", publish.packet_id);

        let payload = publish.payload;
        let topic = util::encode_string(publish.topic);
        let packet_id_present = publish.qos > 0;
        let packet_id_size = if packet_id_present { 2 } else { 0 };

        let mut packet = BytesMut::with_capacity(topic.len() + packet_id_size + payload.len());
        packet.put(topic);

        if packet_id_present {
            packet.put_u16(publish.packet_id);
        }

        packet.put(payload);

        let flags = publish.qos << 1;

        Packet {
            packet_type: PacketType::PUBLISH,
            flags,
            payload: Some(packet.freeze()),
        }
    }
}

impl From<PubAck> for Packet {
    fn from(puback: PubAck) -> Self {
        debug!("creating PUBACK for packet id: {}", puback.packet_id);

        let mut payload = BytesMut::with_capacity(2);
        payload.put_u16(puback.packet_id);

        Packet {
            packet_type: PacketType::PUBACK,
            flags: 0,
            payload: Some(payload.freeze()),
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

impl TryFrom<Packet> for Publish {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::PUBLISH, packet.packet_type);

        let payload = packet.payload.ok_or("malformed")?;

        let qos = packet.flags >> 1 & 3;
        let (topic, payload) = util::take_string(&payload)?;

        let (packet_id, payload) = if qos == 0 { (0, payload) } else { util::take_u18(&payload)? };
        //FIXME
        let payload = Bytes::copy_from_slice(payload);

        Ok(Publish {
            packet_id,
            topic,
            qos,
            payload,
        })
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

#[derive(Eq, Debug, Clone)]
pub struct ApplicationMessage {
    pub id: PacketId,
    pub payload: Bytes,
    pub topic: Topic,
    pub qos: u8,
}

impl From<ApplicationMessage> for Publish {
    fn from(application_message: ApplicationMessage) -> Self {
        Publish {
            packet_id: application_message.id as u16,
            topic: application_message.topic,
            qos: application_message.qos,
            payload: application_message.payload,
        }
    }
}

impl From<ApplicationMessage> for Packet {
    fn from(application_message: ApplicationMessage) -> Self {
        let publish: Publish = application_message.into();
        publish.into()
    }
}

impl Hash for ApplicationMessage {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for ApplicationMessage {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Display for ApplicationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{{packet_id = {}, topic = {}, qos = {}}}", self.id, self.topic, self.qos)
    }
}

impl From<Will> for ApplicationMessage {
    fn from(will: Will) -> Self {
        ApplicationMessage {
            id: 1,
            payload: will.message,
            topic: will.topic,
            qos: will.qos,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_parsing_connect_with_username_only() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\x82\0<\0\x03abc\0\x08username"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect: Connect = packet.try_into().unwrap();
        assert_eq!(4, connect.version);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);

        assert!(connect.will.is_none());

        let auth = connect.auth.unwrap();
        assert_eq!("username".to_string(), auth.username);
        assert_eq!(None, auth.password);
    }

    #[test]
    fn test_parsing_connect_with_will_only() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\x06\0<\0\x03abc\0\x0b/will/topic\0\x0cwill message"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect: Connect = packet.try_into().unwrap();
        assert_eq!(4, connect.version);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);

        let will = connect.will.unwrap();
        assert_eq!("/will/topic".to_string(), will.topic);
        assert_eq!(0, will.qos);
        assert_eq!("will message".to_string(), will.message);

        assert!(connect.auth.is_none());
    }

    #[test]
    fn test_parsing_connect_with_will_and_auth() {
        let packet = Packet {
            payload: Some(Bytes::from(
                &b"\0\x04MQTT\x04\xc6\0<\0\x03abc\0\x0b/will/topic\0\x0cwill message\0\x08username\0\x08password"[..],
            )),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect: Connect = packet.try_into().unwrap();
        assert_eq!(4, connect.version);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);

        let will = connect.will.unwrap();
        assert_eq!("/will/topic".to_string(), will.topic);
        assert_eq!(0, will.qos);
        assert_eq!("will message".to_string(), will.message);

        let auth = connect.auth.unwrap();
        assert_eq!("username".to_string(), auth.username);
        assert_eq!(Some(Bytes::from("password")), auth.password);
    }

    #[test]
    fn test_parsing_connect() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\x02\0<\0\x03abc"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect: Connect = packet.try_into().unwrap();
        assert_eq!(4, connect.version);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);

        assert!(connect.will.is_none());

        assert!(connect.auth.is_none());
    }

    #[test]
    fn test_parsing_publish_qos0() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\n/somethingabc"[..])),
            packet_type: PacketType::PUBLISH,
            flags: 0,
        };

        let publish: Publish = packet.try_into().unwrap();
        assert_eq!(0, publish.qos);
        assert_eq!("/something", publish.topic);
        assert_eq!(Bytes::from("abc"), publish.payload);
    }

    #[test]
    fn test_parsing_publish_qos1() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x0f/something/else\0\x05abc"[..])),
            packet_type: PacketType::PUBLISH,
            flags: 0b0000_0010,
        };

        let publish: Publish = packet.try_into().unwrap();
        assert_eq!(1, publish.qos);
        assert_eq!("/something/else", publish.topic);
        assert_eq!(Bytes::from("abc"), publish.payload);
    }

    #[test]
    fn test_parsing_subscribe() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x01\0\n/something\0"[..])),
            packet_type: PacketType::SUBSCRIBE,
            flags: 0b0000_0010,
        };

        let subscribe: Subscribe = packet.try_into().unwrap();
        assert_eq!(1, subscribe.topics.len());
        assert!(subscribe.topics.contains(&(String::from("/something"), 0)));
    }

    #[test]
    fn test_parsing_subscribe_multiple_topics() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x03\0\x04/qos\0\0\x0f/something/else\x01"[..])),
            packet_type: PacketType::SUBSCRIBE,
            flags: 0b0000_0010,
        };

        let subscribe: Subscribe = packet.try_into().unwrap();
        assert_eq!(2, subscribe.topics.len());
        assert!(subscribe.topics.contains(&(String::from("/qos"), 0)));
        assert!(subscribe.topics.contains(&(String::from("/something/else"), 1)));
    }

    #[test]
    fn test_connect() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b0000_0000);
        assert_eq!(header, 0b0001_0000);
    }

    #[test]
    fn test_connect_with_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b0000_0001);
        assert_eq!(header, 0b0001_0001);
    }

    #[test]
    #[should_panic]
    fn test_connect_with_invalid_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNECT, 0b1000_0001);
        assert_eq!(header, 0b0001_0001);
    }

    #[test]
    fn test_connack_flags() {
        let header = Packet::type_and_flags(&PacketType::CONNACK, 0b0000_0000);
        assert_eq!(header, 0b0010_0000);
    }

    #[test]
    fn test_publish() {
        let header = Packet::type_and_flags(&PacketType::PUBLISH, 0b0000_1111);
        assert_eq!(header, 0b0011_1111);
    }

    #[test]
    fn test_connack() {
        let conn_ack: Packet = ConnAck::default().into();
        assert_eq!(
            Bytes::from(&[0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000][..]),
            Into::<Bytes>::into(conn_ack)
        );
    }
}
