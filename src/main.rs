mod codec;
mod broker;

extern crate bytes;
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate tokio_core;
extern crate tokio_io;

#[cfg(test)]
extern crate matches;

#[cfg(test)]
extern crate test;

use broker::{Broker, Client};
use futures::{Future, Stream};
use log::LogLevel;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use std::io;
use std::rc::Rc;
use codec::{Type, MQTTPacket};
use futures::sync::mpsc::UnboundedReceiver;

pub struct MQTTCodec;

fn main() {
    simple_logger::init_with_level(LogLevel::Info).unwrap();
    info!("raqqietka starting");

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let addr = "127.0.0.1:1883".parse().unwrap();
    let listener = TcpListener::bind(&addr, &handle).unwrap();
    let broker = Rc::new(Broker::new());

    let connections = listener.incoming();
    let server = connections
        .for_each(move |(stream, _addr)| {
            //For each new connection use MQTTCodec to parse traffic
            let framed = stream.framed(MQTTCodec);
            let handle_inner = handle.clone();

            let broker = Rc::clone(&broker);
            let mqtt = framed
                .into_future()
                .map_err(|(err, _)| err)
                .and_then(move |(packet, framed)| parse_connect(&broker, packet, framed))
                .map(move |(framed, rx, client, client_id, connack)| {
                    //queue the CONNACK
                    client.send(connack);

                    //handle incoming packets
                    info!("Handling requests for client: {:?}", client_id);

                    let (sender, receiver) = framed.split();

                    //Redirect client's receiver to "global" sender
                    let tx_future = rx
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "something went wrong"))
                        .forward(sender)
                        .then(move |_| Ok::<_, ()>(()));

                    let responses = receiver
                        .for_each(move |msg| handle_msg(msg, &client))
                        .then(move |_| {
                            info!("what's going on in here?");
                            Ok::<_, ()>(())
                        })
                        //combine receiver and transmitter?
                        .select(tx_future)
                        .then(move |_| {
                            info!("disconnecting client: {:?}", client_id);
                            Ok::<_, ()>(())
                        });

                    //handle "inner" futures
                    handle_inner.spawn(responses);
                    Ok::<_, ()>(())
                })
                .then(|_| {
                    info!("and here?");
                    Ok(())
                });

            //handle all futures
            handle.spawn(mqtt);
            Ok(())
        });

    core.run(server).unwrap();
}

fn parse_connect(broker: &Broker, packet: Option<MQTTPacket>, framed: Framed<TcpStream, MQTTCodec>) -> Result<(Framed<TcpStream, MQTTCodec>, UnboundedReceiver<MQTTPacket>, Client, String, MQTTPacket), io::Error> {
    //initiate the MQTT connection by handling first package (and_then)
    if let Some(MQTTPacket { packet: Type::CONNECT(client_id, username, password, will), }) = packet {
        info!("connected: {:?}, {:?}, {:?}, {:?}", client_id, username, password, will);
        let (client, rx) = broker.connect(client_id.clone());

        Ok((framed, rx, client, client_id, MQTTPacket::connack()))
    } else {
        error!("error");
        Err(io::Error::new(io::ErrorKind::Other, "unknown packet"))
    }
}

fn handle_msg(msg: MQTTPacket, client: &Client) -> Result<(), io::Error> {
    //handle incoming packets
    info!("new msg: {:?} for client: {:?}", msg, client);

    let response = match msg.packet {
        Type::PUBLISH(Some(packet_identifier), _topic, _qos_level, _payload) => MQTTPacket::puback(packet_identifier),
        Type::PUBLISH(None, _topic, 0, _payload) => MQTTPacket::none(),
        Type::SUBSCRIBE(packet_identifier, topics) => MQTTPacket::suback(packet_identifier, topics.iter().map(|topic| topic.1).collect()),
        //Type::DISCONNECT => codec::MQTTResponse::none(),
        _ => MQTTPacket::none(),
    };

    client.send(response);
    Ok(())
}
