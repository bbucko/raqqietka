extern crate bytes;
#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate num_traits;
extern crate tokio;
extern crate rocksdb;

use std::sync::{Arc, Mutex};

use futures::future::{self, Either};
use log::Level;
use tokio::net::TcpListener;
use tokio::prelude::*;

use mqtt::*;

mod mqtt;

fn main() -> Result<(), Box<std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();

    let bind_addr = "127.0.0.1:1883".parse()?;
    let listener = TcpListener::bind(&bind_addr)?;
    info!("raqqietka starting on {}", bind_addr);

    let broker = Arc::new(Mutex::new(Broker::new()));

    let server = listener
        .incoming()
        .map_err(|e| error!("Client tried to connect and failed: {:?}", e))
        .for_each(move |socket| {
            let packets = mqtt::Packets::new(socket);

            let broker = broker.clone();

            let connection = packets
                .into_future()
                .map_err(|(e, _)| e)
                .and_then(|(connect, packets)| {
                    let connect = match connect {
                        Some(connect) => connect,
                        None => {
                            return Either::A(future::ok(()));
                        }
                    };

                    let client = Client::new(connect, broker, packets);
                    Either::B(client)
                })
                .map_err(|e| {
                    error!("Connection error = {:?}", e);
                });

            tokio::spawn(connection)
        });

    tokio::run(server);

    Ok(())
}
