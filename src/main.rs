mod codec;

#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

use std::io;
use futures::{future, Future};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use tokio_proto::pipeline::ServerProto;
use tokio_proto::TcpServer;
use tokio_service::Service;

pub struct MQTTProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for MQTTProto {
    // For this protocol style, `Request` matches the `Item` type of the codec's `Decoder`
    type Request = String;

    // For this protocol style, `Response` matches the `Item` type of the codec's `Encoder`
    type Response = String;

    // A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, codec::MQTTCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(codec::MQTTCodec))
    }
}

pub struct MQTT;

impl Service for MQTT {
    // These types must match the corresponding protocol types:
    type Request = String;
    type Response = String;

    // For non-streaming protocols, service errors are always io::Error
    type Error = io::Error;

    // The future for computing the response; box it for simplicity.
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    // Produce a future for computing a response from a request.
    fn call(&self, req: Self::Request) -> Self::Future {
        // In this case, the response is immediate.
        Box::new(future::ok(req))
    }
}


fn main() {
    info!("raqqietka starting");
    simple_logger::init_with_level(log::LogLevel::Info).unwrap();

    let addr = "127.0.0.1:1883".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(MQTTProto, addr);

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    server.serve(|| Ok(MQTT));
}