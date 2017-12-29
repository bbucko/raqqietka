mod codec;

#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_service;
extern crate tokio_core;

#[cfg(test)]
extern crate matches;

use std::io;
use std::rc;
use std::net;
use std::collections;
use std::cell;
use tokio_io::AsyncRead;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_service::{Service, NewService};
use futures::{Future, Stream, Sink};

struct Client;

struct ConnectedClients(rc::Rc<cell::RefCell<collections::HashMap<net::SocketAddr, Client>>>);

pub struct MQTT;

pub struct MQTTCodec;

fn main() {
    simple_logger::init_with_level(log::LogLevel::Info).unwrap();
    info!("raqqietka starting");

    if let Err(e) = serve(|| Ok(MQTT)) {
        println!("Server failed with {}", e);
    }
}

fn serve<S>(s: S) -> io::Result<()> where S: NewService<Request=codec::MQTTRequest, Response=codec::MQTTResponse, Error=io::Error> + 'static {
    let mut core = Core::new()?;
    let handle = core.handle();

    let addr = "127.0.0.1:1883".parse().unwrap();
    let listener = TcpListener::bind(&addr, &handle)?;

    let connections = listener.incoming();
    let server = connections.for_each(move |(socket, addr)| {
        let (writer, reader) = socket.framed(MQTTCodec::new()).split();

        let service = s.new_service()?;

        let responses = reader.and_then(move |req| service.call(req));
        let server = writer.send_all(responses)
            .then(move |_| {
                info!("Client {:?} disconnected", addr);
                Ok(())
            });

        handle.spawn(server);

        Ok(())
    });

    core.run(server)
}




