use std::collections::HashSet;
use std::convert::TryFrom;

use bytes::Bytes;

use crate::{util, Connect, ConnectAuth, MQTTError, Packet, PacketType, Publish, Subscribe, Unsubscribe, Will};

impl TryFrom<Packet> for Connect {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::CONNECT, packet.packet_type);

        let payload = packet.payload.ok_or("malformed payload")?;

        let (proto_name, payload) = util::take_string(&payload)?;
        assert_eq!(proto_name.as_str(), "MQTT");

        let (version, payload) = payload.split_first().ok_or("malformed version")?;
        let (flags, payload) = payload.split_first().ok_or("malformed flags")?;
        let flags = *flags;

        let clean_session = util::check_flag(flags, 1);
        let (_keep_alive, payload) = util::take_u18(&payload)?;

        let (client_id, mut payload) = util::take_string(&payload)?;

        let will_flag = util::check_flag(flags, 2);
        let will_retain = util::check_flag(flags, 5);
        let will_qos: u8 = (flags >> 3) & 3u8;

        let will = if will_flag {
            let internal_payload = payload;

            let (will_topic, internal_payload) = util::take_string(&internal_payload)?;
            let (will_length, internal_payload) = util::take_u18(&internal_payload)?;
            let (will_payload, internal_payload) = internal_payload.split_at(will_length as usize);

            payload = internal_payload;

            Some(Will {
                qos: will_qos,
                retain: will_retain,
                topic: will_topic,
                message: Bytes::from(will_payload),
            })
        } else {
            if will_retain || will_qos != 0 {
                return Err(format!("malformed will: retain: {}; qos: {}", will_retain, will_qos).into());
            }

            None
        };

        let username_flag = util::check_flag(flags, 7);

        let auth = if username_flag {
            let internal_payload = payload;
            let (username, internal_payload) = util::take_string(&internal_payload)?;

            let password_flag = util::check_flag(flags, 6);
            let password = if password_flag {
                let (password_length, internal_payload) = util::take_u18(&internal_payload)?;
                let (password, internal_payload) = internal_payload.split_at(password_length as usize);

                payload = internal_payload;
                Some(Bytes::from(password))
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
            version: *version,
            client_id: Some(client_id),
            auth,
            will,
            clean_session,
        })
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
        let payload = Bytes::from(payload);

        Ok(Publish {
            packet_id,
            topic,
            qos,
            payload,
        })
    }
}

impl TryFrom<Packet> for Subscribe {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::SUBSCRIBE, packet.packet_type);

        let payload = packet.payload.ok_or("malformed")?;
        let (packet_id, mut payload) = util::take_u18(&payload)?;

        let mut topics = HashSet::new();

        loop {
            if payload.is_empty() {
                break;
            }

            let topic_payload = payload;

            let (topic, topic_payload) = util::take_string(&topic_payload)?;

            let (qos, topic_payload) = topic_payload.split_first().ok_or("malformed")?;

            topics.insert((topic, *qos));
            payload = topic_payload;
        }

        Ok(Subscribe { packet_id, topics })
    }
}

impl TryFrom<Packet> for Unsubscribe {
    type Error = MQTTError;

    fn try_from(packet: Packet) -> Result<Self, Self::Error> {
        assert_eq!(PacketType::UNSUBSCRIBE, packet.packet_type);

        let payload = packet.payload.ok_or("malformed")?;
        let (packet_id, mut payload) = util::take_u18(&payload)?;

        let mut topics = HashSet::new();

        loop {
            if payload.is_empty() {
                break;
            }

            let topic_payload = payload;

            let (topic, topic_payload) = util::take_string(&topic_payload)?;

            topics.insert(topic);
            payload = topic_payload;
        }

        Ok(Unsubscribe { packet_id, topics })
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
}
