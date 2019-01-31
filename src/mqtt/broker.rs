use std::collections::HashMap;
use std::collections::HashSet;
use std::io;

use bytes::Bytes;

use mqtt::*;

fn take_u18(bytes: &[u8]) -> Result<(u16, &[u8]), ()> {
    if bytes.len() < 2 { return Err(()); }
    let (length_bytes, bytes) = bytes.split_at(2);
    Ok(((u16::from(length_bytes[0]) << 8) + u16::from(length_bytes[1]), bytes))
}

fn take_string(bytes: &[u8]) -> Result<(String, &[u8]), io::Error> {
    let (string_length, bytes) = take_u18(bytes).map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid string length"))?;
    let (string_bytes, bytes) = bytes.split_at(string_length as usize);
    let string = String::from_utf8(string_bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok((string, bytes))
}

pub struct Connect {
    pub proto: Option<String>,
    pub version: u8,
    pub client_id: Option<ClientId>,
    pub auth: Option<ConnectAuth>,
    pub will: Will,
    pub clean_session: bool,
}

pub struct ConnectAuth {}

pub struct Will {}

impl Connect {
    fn empty() -> Self {
        Connect {
            proto: None,
            version: 0,
            client_id: None,
            auth: None,
            will: Will {},
            clean_session: false,
        }
    }
}

impl From<Packet> for Connect {
    fn from(packet: Packet) -> Self {
        assert_eq!(PacketType::CONNECT, packet.packet_type);

        let payload = packet.payload.expect("Missing header");

        let (proto_name, payload) = match take_string(&payload) {
            Ok(val) => val,
            Err(_) => { return Connect::empty(); }
        };

        let (version, payload) = match payload.split_first() {
            Some(val) => val,
            None => { return Connect::empty(); }
        };

        let (flags, payload) = match payload.split_first() {
            Some(val) => val,
            None => { return Connect::empty(); }
        };

        let clean_session = ((*flags) & 2u8) >> 1 == 1;
        let (_keep_alive, payload) = match take_u18(&payload) {
            Ok(val) => val,
            Err(_) => { return Connect::empty(); }
        };

        let (client_id, _payload) = match take_string(&payload) {
            Ok(val) => val,
            Err(_) => { return Connect::empty(); }
        };

        Connect {
            proto: Some(proto_name),
            version: *version,
            client_id: Some(client_id),
            auth: None,
            will: Will {},
            clean_session,
        }
    }
}

impl Publish {
    fn empty() -> Publish {
        Publish {
            packet_id: 0,
            payload: Bytes::new(),
            topic: String::new(),
            qos: 0,
        }
    }
}

impl From<Packet> for Publish {
    fn from(packet: Packet) -> Self {
        assert_eq!(PacketType::PUBLISH, packet.packet_type);

        let payload = packet.payload.expect("missing payload");

        let qos = packet.flags >> 1 & 3;
        let (topic, payload) = match take_string(&payload) {
            Ok(val) => val,
            Err(_) => { return Publish::empty(); }
        };

        let (packet_id, payload) =
            if qos == 0 {
                (0, payload)
            } else {
                match take_u18(&payload) {
                    Ok((packet_id, payload)) => (packet_id, payload),
                    Err(_) => { return Publish::empty(); }
                }
            };

        Publish {
            packet_id,
            topic,
            qos,
            payload: Bytes::from(payload),
        }
    }
}

pub struct Publish {
    pub packet_id: u16,
    pub topic: Topic,
    pub qos: u8,
    pub payload: Bytes,
}

impl Subscribe {
    fn empty() -> Self {
        Subscribe {
            packet_id: 0,
            topics: HashSet::new(),
        }
    }
}

impl From<Packet> for Subscribe {
    fn from(packet: Packet) -> Self {
        assert_eq!(PacketType::SUBSCRIBE, packet.packet_type);

        let payload = packet.payload.expect("missing payload");
        let (packet_id, mut payload) = match take_u18(&payload) {
            Ok((packet_id, payload)) => (packet_id, payload),
            Err(_) => { return Subscribe::empty(); }
        };

        let mut topics = HashSet::new();

        loop {
            if payload.is_empty() { break; }

            let topic_payload = payload;

            let (topic, topic_payload) = match take_string(&topic_payload) {
                Ok(val) => val,
                Err(_) => { return Subscribe::empty(); }
            };

            let (qos, topic_payload) = match topic_payload.split_first() {
                Some(val) => val,
                None => { return Subscribe::empty(); }
            };

            topics.insert((topic, *qos));
            payload = topic_payload;
        }

        Subscribe { packet_id, topics }
    }
}

pub struct Subscribe {
    pub packet_id: u16,
    pub topics: HashSet<(Topic, u8)>,
}


impl Broker {
    pub fn new() -> Self {
        info!("Broker has started");
        Broker {
            clients: HashMap::new(),
            subscriptions: HashMap::new(),
        }
    }

    pub fn connect(&mut self, connect: Connect, tx: Tx) -> ClientId {
        let client_id = connect.client_id.unwrap();
        info!("Broker has connected new client: {:?}", &client_id);
        self.clients.insert(client_id.to_owned(), tx);
        client_id
    }

    pub fn disconnect(&mut self, client_id: ClientId) {
        info!("Broker has disconnected client: {:?}", client_id);
        self.clients.remove(&client_id);
    }

    pub fn subscribe(&mut self, client: &Client, subscribe: Subscribe) -> Result<(), io::Error> {
        for (topic, _qos) in subscribe.topics {
            info!("Broker has subscribed client {} to topic: {}", client, topic);
            let subscribed_clients = self.subscriptions
                .entry(topic.to_owned())
                .or_insert_with(HashSet::new);

            subscribed_clients.insert(client.client_id.clone());
        }


        Ok(())
    }

    pub fn publish(&mut self, publish: Publish) -> Result<(), io::Error> {
        let clients = &self.clients;

        self.subscriptions
            .entry(publish.topic.clone()).or_default()
            .retain(|client|
                if let Some(tx) = clients.get(client) {
                    let publish_packet = Packet::publish(publish.topic.to_owned(), publish.payload.clone(), publish.qos);
                    info!("Sending payload: {} to client: {}", publish_packet, client);
                    let _ = tx.unbounded_send(publish_packet);
                    true
                } else {
                    info!("Removing disconnected client: {}", client);
                    false
                }
            );

        Ok(())
    }
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
    fn test_parsing_connect() {
        let connect_packet = Packet {
            payload: Some(Bytes::from(&b"\0\x04MQTT\x04\xc6\0<\0\x03abc\0\x0b/will/topic\0\x0cwill message\0\x08username\0\x08password"[..])),
            packet_type: PacketType::CONNECT,
            flags: 0,
        };

        let connect: Connect = connect_packet.into();
        assert_eq!(4, connect.version);
        assert_eq!(Some("MQTT".to_string()), connect.proto);
        assert_eq!(true, connect.clean_session);
        assert_eq!(Some("abc".to_string()), connect.client_id);
    }

    #[test]
    fn test_parsing_publish_qos0() {
        let publish_packet = Packet {
            payload: Some(Bytes::from(&b"\0\n/somethingabc"[..])),
            packet_type: PacketType::PUBLISH,
            flags: 0,
        };

        let publish: Publish = publish_packet.into();
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

        let publish: Publish = packet.into();
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

        let subscribe: Subscribe = packet.into();
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

        let subscribe: Subscribe = packet.into();
        assert_eq!(2, subscribe.topics.len());
        assert!(subscribe.topics.contains(&(String::from("/qos"), 0)));
        assert!(subscribe.topics.contains(&(String::from("/something/else"), 1)));
    }
}