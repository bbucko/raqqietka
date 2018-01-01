mod codec;

extern crate bytes;
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;

extern crate tokio_core;
extern crate tokio_io;

#[cfg(test)]
extern crate matches;

use futures::{Future, Sink, Stream};
use log::LogLevel;
use std::io;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;

pub struct MQTTCodec;

#[derive(Debug)]
struct Client;

fn main() {
    simple_logger::init_with_level(LogLevel::Trace).unwrap();
    info!("raqqietka starting");

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let addr = "127.0.0.1:1883".parse().unwrap();
    let listener = TcpListener::bind(&addr, &handle).unwrap();

    let connections = listener.incoming();
    let server = connections.for_each(move |(stream, _addr)| {
        let framed = stream.framed(MQTTCodec);
        let handle_inner = handle.clone();

        let handshake = framed.into_future().map_err(|(err, _)| err).and_then(
            move |(packet, framed)| if let Some(codec::MQTTRequest { packet: codec::Type::CONNECT(client_id, username, password, will), }) = packet {
                let client = Client;
                info!("connected: {:?}, {:?}, {:?}, {:?}", client_id, username, password, will);

                let (tx, rx) = futures::sync::mpsc::unbounded::<codec::MQTTResponse>();
                Ok((framed, rx, tx, client, codec::MQTTResponse::connack()))
            } else {
                error!("error");
                Err(io::Error::new(io::ErrorKind::Other, "unknown packet"))
            },
        );

        let mqtt = handshake
            .map(move |(framed, rx, tx, client, connack)| {
                let (sender, receiver) = framed.split();
                if let Err(err) = tx.clone().send(connack).wait() {
                    error!("something went wrong: {:?}", err);
                }

                let rx_future = receiver
                    .for_each(move |msg: codec::MQTTRequest| {
                        info!("new msg: {:?}", msg);
                        let response = match msg.packet {
                            codec::Type::CONNECT(_client_id, _username, _password, _will) => codec::MQTTResponse::connack(),
                            codec::Type::PUBLISH(Some(packet_identifier), _topic, _qos_level, _payload) => codec::MQTTResponse::puback(packet_identifier),
                            codec::Type::PUBLISH(None, _topic, 0, _payload) => codec::MQTTResponse::none(),
                            codec::Type::SUBSCRIBE(packet_identifier, topics) => codec::MQTTResponse::suback(packet_identifier, topics.iter().map(|topic| topic.1).collect()),
                            //                            codec::Type::DISCONNECT => codec::MQTTResponse::none(),
                            _ => codec::MQTTResponse::none(),
                        };

                        if let Err(msg) = tx.clone().unbounded_send(response) {
                            error!("Error occurred while sending: {:?}", msg);
                        }

                        Ok(())
                    })
                    .then(move |_| Ok::<_, ()>(()));

                let tx_future = rx.map_err(|e| {
                    error!("Channel error = {:?}", e);
                    io::Error::new(io::ErrorKind::Other, "unknown packet")
                })
                    .forward(sender)
                    .then(move |_| Ok::<_, ()>(()));

                let connection = rx_future.select(tx_future).then(move |_| {
                    error!("disconnecting client: {:?}", client);
                    Ok::<_, ()>(())
                });

                handle_inner.spawn(connection);
                Ok::<_, ()>(())
            })
            .then(|_| Ok(()));

        handle.spawn(mqtt);
        Ok(())
    });

    core.run(server).unwrap();
}
