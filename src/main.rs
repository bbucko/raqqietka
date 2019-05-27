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

use std::error;
use std::fmt::{Display, Error, Formatter};
use std::sync::{Arc, Mutex, PoisonError};

use futures::future::{self, Either};
use log::Level;
use tokio::net::TcpListener;
use tokio::prelude::*;

use broker::*;
use mqtt::*;
use MQTTError::OtherError;

mod broker;
mod mqtt;

#[derive(Debug)]
pub enum MQTTError {
    ClientError,
    ServerError(String),
    OtherError(String),
}

impl Display for MQTTError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match *self {
            OtherError(ref err) => write!(f, "{}", err),
            _ => write!(f, "Error"),
        }
    }
}

impl error::Error for MQTTError {}

impl std::convert::From<std::sync::PoisonError<std::sync::MutexGuard<'_, broker::Broker>>> for MQTTError {
    fn from(err: PoisonError<std::sync::MutexGuard<'_, broker::Broker>>) -> Self {
        MQTTError::ServerError(err.to_string())
    }
}

impl From<&str> for MQTTError {
    fn from(str: &str) -> Self {
        MQTTError::OtherError(String::from(str))
    }
}

impl From<String> for MQTTError {
    fn from(str: String) -> Self {
        MQTTError::OtherError(str)
    }
}

fn main() -> Result<(), Box<std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();

    let bind_addr = "127.0.0.1:1883".parse()?;
    let listener = TcpListener::bind(&bind_addr)?;
    info!("raqqietka starting on {}", bind_addr);

    let broker = Arc::new(Mutex::new(Broker::new()));

    let server = listener
        .incoming()
        .map_err(|e| error!("Client tried to connect and failed: {:?}", e))
        .map(mqtt::Packets::from)
        .for_each(move |packets| {
            let broker = broker.clone();
            let connection = packets
                .into_future()
                .map_err(|(e, _)| e) //why this is needed?
                .and_then(|(connect, packets)| {
                    let inner_broker = broker.clone();
                    match connect {
                        Some(connect) => {
                            match Client::new(connect, broker, packets) {
                                Ok((mut client, tx)) => {
                                    let client_id = client.client_id.clone();

                                    if inner_broker.lock()
                                        .map_err(|err| MQTTError::ServerError(err.to_string()))
                                        .and_then(|mut broker| broker.register(&client_id, tx))
                                        .is_ok() {
                                        client.send_connack();
                                        return Either::A(client);
                                    }
                                }
                                Err(err) => {
                                    error!("registration failed: {}", err);
                                }
                            }
                        }
                        None => {
                            error!("missing CONNECT packet");
                        }
                    };
                    Either::B(future::ok(()))
                })
                .map_err(|e| {
                    error!("connection error: {:?}", e);
                });

            tokio::spawn(connection)
        });

    tokio::run(server);

    Ok(())
}
