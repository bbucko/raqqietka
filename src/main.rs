extern crate bytes;
#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate num_traits;
extern crate regex_cache;
extern crate tokio;

use std::sync::{Arc, Mutex};

use futures::future::{self, Either};
use log::Level;
use tokio::net::TcpListener;
use tokio::prelude::*;

use broker::*;
use mqtt::*;

mod broker;
mod mqtt;

pub type MQTTError = String;

fn main() -> Result<(), Box<std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();

    let bind_addr = "127.0.0.1:1883".parse()?;
    let listener = TcpListener::bind(&bind_addr)?;
    info!("raqqietka starting on {}", bind_addr);

    let broker = Arc::new(Mutex::new(Broker::new()));

    let server = listener
        .incoming()
        .map_err(|e| error!("Client tried to connect and failed: {:?}", e))
        .map(mqtt::Packets::new)
        .inspect(|a| debug!("New connection: {:?}", a))
        .for_each(move |packets| {
            let broker = broker.clone();

            let connection = packets
                .into_future()
                .map_err(|(e, _)| e.to_string())
                .and_then(|(connect, packets)| match connect {
                    Some(connect) => {
                        let inner_broker = broker.clone();
                        if let Ok((mut client, tx)) = Client::new(connect, broker, packets) {
                            let client_id = client.client_id.clone();

                            if inner_broker.lock().expect("missing broker").register(&client_id, tx).is_ok() {
                                client.send_connack();
                                return Either::A(client);
                            }
                        };

                        error!("registration failed");
                        Either::B(future::ok(()))
                    }
                    None => {
                        error!("something went wrong");
                        Either::B(future::ok(()))
                    }
                })
                .map_err(|e| {
                    error!("connection error: {:?}", e);
                });

            tokio::spawn(connection)
        });

    tokio::run(server);

    Ok(())
}
