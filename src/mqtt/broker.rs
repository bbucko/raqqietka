use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::ErrorKind;

use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};

use mqtt::*;

impl Broker {
    pub fn new() -> Self {
        info!("Broker has started");

        let cf = ColumnFamilyDescriptor::new("clients", Options::default());

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors(&db_opts, "/tmp/rocksdb", vec![cf]).unwrap();

        Broker {
            clients: HashMap::new(),
            subscriptions: HashMap::new(),
            db,
        }
    }

    pub fn connect(&mut self, connect: Connect, tx: Tx) -> ClientId {
        let client_id = connect.client_id.unwrap();
        info!("Client: {:?} has connected to broker", &client_id);
        self.clients.insert(client_id.to_owned(), tx);

        let cf_handle = self.db.cf_handle("clients").unwrap();
        match self.db.put_cf(cf_handle, client_id.clone().as_bytes(), b"{}") {
            Ok(()) => println!("Created OK"),
            _ => println!("Error")
        };

        client_id
    }

    pub fn disconnect(&mut self, client_id: ClientId) {
        info!("Client: {:?} has disconnected from broker", client_id);

        let cf_handle = self.db.cf_handle("clients").unwrap();
        let _result = match self.db.delete_cf(cf_handle, client_id.clone().as_bytes()) {
            Ok(()) => self.clients.remove(&client_id),
            _ => None
        };
    }

    pub fn subscribe(&mut self, client: &Client, subscribe: Subscribe) -> Result<Vec<u8>, io::Error> {
        let cf_handle = self.db.cf_handle("clients").unwrap();
        let _result = match self.db.get_cf(cf_handle, client.client_id.as_bytes()) {
            Ok(Some(result)) => result,
            _ => return Err(io::Error::new(ErrorKind::Other, "uh oh"))
        };

        let mut result = vec!();
        for (topic, qos) in subscribe.topics {
            info!("Client: {} has subscribed to topic: {} with qos {}", client, topic, qos);
            self.subscriptions
                .entry(topic.to_owned())
                .or_insert_with(HashSet::new)
                .insert(client.client_id.clone());

            result.push(qos);
        }

        Ok(result)
    }

    pub fn publish(&mut self, publish: Publish) -> Result<(), io::Error> {
        //enqueue for publishing
        //save to LSM
        let clients = &self.clients;

        self.subscriptions.entry(publish.topic.clone()).or_default().retain(|client| {
            if let Some(tx) = clients.get(client) {
                let publish_packet = Packet::publish(publish.packet_id, publish.topic.to_owned(), publish.payload.clone(), publish.qos);
                info!("Sending payload: {} to client: {}", publish_packet, client);
                let _ = tx.unbounded_send(publish_packet);
                true
            } else {
                info!("Removing disconnected client: {}", client);
                false
            }
        });

        Ok(())
    }
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
    username: String,
    password: Option<Bytes>,
}

#[derive(Debug)]
pub struct Will {
    qos: u8,
    retain: bool,
    topic: String,
    message: Bytes,
}

impl Connect {
    pub fn from(packet: Packet) -> Result<Self, io::Error> {
        //rewrite to try_from
        assert_eq!(PacketType::CONNECT, packet.packet_type);

        let payload = packet.payload.ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed payload"))?;

        let (proto_name, payload) = util::take_string(&payload)?;
        assert_eq!(proto_name, "MQTT".to_string());

        let (version, payload) = payload.split_first().ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed version"))?;
        let (flags, payload) = payload.split_first().ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed flags"))?;

        let clean_session = util::check_flag(flags, 1);
        let (_keep_alive, payload) = util::take_u18(&payload)?;

        let (client_id, mut payload) = util::take_string(&payload)?;

        if client_id.is_empty() || client_id.len() > 23 {
            return Err(io::Error::new(ErrorKind::Other, "malformed client_id invalid length"));
        }

        if !client_id.chars().all(|c| c.is_alphanumeric()) {
            //return 0x02
            return Err(io::Error::new(ErrorKind::Other, "malformed client_id invalid characters"));
        }

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
                return Err(io::Error::new(ErrorKind::Other, "malformed will"));
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

impl Publish {
    pub fn from(packet: Packet) -> Result<Self, io::Error> {
        //rewrite to try_from
        assert_eq!(PacketType::PUBLISH, packet.packet_type);

        let payload = packet.payload.ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed"))?;

        let qos = packet.flags >> 1 & 3;
        let (topic, payload) = util::take_string(&payload)?;

        let (packet_id, payload) = if qos == 0 {
            (0, payload)
        } else {
            util::take_u18(&payload)?
        };

        Ok(Publish {
            packet_id,
            topic,
            qos,
            payload: Bytes::from(payload),
        })
    }
}

pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

impl Subscribe {
    pub fn from(packet: Packet) -> Result<Self, io::Error> {
        //rewrite to try_from
        assert_eq!(PacketType::SUBSCRIBE, packet.packet_type);

        let payload = packet.payload.ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed"))?;
        let (packet_id, mut payload) = util::take_u18(&payload)?;

        let mut topics = HashSet::new();

        loop {
            if payload.is_empty() {
                break;
            }

            let topic_payload = payload;

            let (topic, topic_payload) = util::take_string(&topic_payload)?;

            let (qos, topic_payload) = topic_payload.split_first().ok_or_else(|| io::Error::new(ErrorKind::Other, "malformed"))?;

            topics.insert((topic, *qos));
            payload = topic_payload;
        }

        Ok(Subscribe { packet_id, topics })
    }
}

pub struct Subscribe {
    pub packet_id: u16,
    pub topics: HashSet<(Topic, u8)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_connect_with_username_only() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\x82\0<\0\x03abc\0\x08username"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect = Connect::from(packet).unwrap();
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

        let connect = Connect::from(packet).unwrap();
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
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\xc6\0<\0\x03abc\0\x0b/will/topic\0\x0cwill message\0\x08username\0\x08password"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect = Connect::from(packet).unwrap();
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

        let connect = Connect::from(packet).unwrap();
        assert_eq!(4, connect.version);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);

        assert!(connect.will.is_none());

        assert!(connect.auth.is_none());
    }

    #[test]
    fn test_parsing_connect_with_invalid_characters() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\x02\0<\0\x03;bc"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };
        let result = Connect::from(packet);

        assert!(result.is_err());
        assert_eq!("malformed client_id invalid characters", result.err().unwrap().to_string());
    }

    #[test]
    fn test_parsing_publish_qos0() {
        let packet = Packet {
            payload: Some(Bytes::from(&b"\0\n/somethingabc"[..])),
            packet_type: PacketType::PUBLISH,
            flags: 0,
        };

        let publish = Publish::from(packet).unwrap();
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

        let publish = Publish::from(packet).unwrap();
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

        let subscribe = Subscribe::from(packet).unwrap();
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

        let subscribe = Subscribe::from(packet).unwrap();
        assert_eq!(2, subscribe.topics.len());
        assert!(subscribe.topics.contains(&(String::from("/qos"), 0)));
        assert!(subscribe.topics.contains(&(String::from("/something/else"), 1)));
    }
}
