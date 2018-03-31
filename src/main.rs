#![deny(warnings)]
extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate tokio;

use broker::Broker;
use codec::MQTT as Codec;
use futures::{Future, Stream};
use futures::future::{self, Either};
use log::LogLevel;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};

mod broker;
mod client;
mod codec;

fn handle_error(e: io::Error) {
    error!("connection error = {:?}", e);
}

fn process(socket: TcpStream, broker: Arc<Broker>) -> Box<Future<Item=(), Error=()> + Send> {
    info!("new connection accepted from: {:?} to broker: {:?}", socket.peer_addr(), broker);
    let addr = socket.peer_addr().unwrap();
    let msg = Codec::new(socket)
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(connect, packets)| {
            info!("new client connected: {:?}", connect);
            match connect {
                Some(connect) => {
                    if let Some(client) = Broker::register(&connect, packets, &broker, addr) {
                        return Either::A(client);
                    } else {
                        return Either::B(future::ok(()));
                    }
                }
                None => Either::B(future::ok(())),
            }
        })
        .map_err(handle_error);

    Box::new(msg)
}

fn main() {
    simple_logger::init_with_level(LogLevel::Info).unwrap();

    let addr_str = format!("127.0.0.1:{}", port());
    info!("raqqietka starting on {}", addr_str);

    let addr = addr_str.parse().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();

    let broker = Arc::new(Broker::new());

    let server = listener
        .incoming()
        .map_err(|e| error!("failed to accept socket; error = {:?}", e))
        .for_each(move |socket| tokio::spawn(process(socket, broker.clone())));

    tokio::run(server);
}

fn port() -> String {
    match std::env::var("process.env.PORT") {
        Ok(val) => val,
        _ => "1883".to_string(),
    }
}
