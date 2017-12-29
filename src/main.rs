mod codec;

#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate tokio_core;

#[cfg(test)]
extern crate matches;

use std::io;
use tokio_io::AsyncRead;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_service::{Service, NewService};
use futures::{future, Future, Stream, Sink};

pub struct MQTT;

impl Service for MQTT {
    // These types must match the corresponding protocol types:
    type Request = codec::MQTTRequest;
    type Response = codec::MQTTResponse;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        // In this case, the response is immediate.
        info!("Request: {:?}", req);
        let response = Box::new(future::ok(codec::MQTTResponse {}));
        info!("Response: {:?}", response);
        response
    }
}

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
    let server = connections.for_each(move |(socket, _peer_addr)| {
        let (writer, reader) = socket.framed(codec::MQTTCodec::new()).split();
        let service = s.new_service()?;

        let responses = reader.and_then(move |req| service.call(req));
        let server = writer.send_all(responses)
            .then(|_| Ok(()));
        handle.spawn(server);

        Ok(())
    });

    core.run(server)
}




