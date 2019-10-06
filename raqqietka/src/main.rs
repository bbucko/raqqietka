#![warn(rust_2018_idioms)]

#[macro_use]
extern crate log;

use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;

use log::Level;
use tokio::codec::Framed;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::Mutex;

use broker::{Broker, Connect};
use mqtt::{Client, PacketsCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init_with_level(Level::Info).unwrap();

    let bind_addr = "127.0.0.1:1883";
    let mut listener = TcpListener::bind(&bind_addr).await?;
    info!("raqqietka starting on {}", bind_addr);

    let broker = Arc::new(Mutex::new(Broker::new()));

    loop {
        let (stream, addr) = listener.accept().await?;
        let broker = Arc::clone(&broker);

        tokio::spawn(async move {
            if let Err(e) = process(broker, stream, addr).await {
                println!("an error occurred; error = {:?}", e);
            }
        });
    }
}

async fn process(broker: Arc<Mutex<Broker>>, stream: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut packets = Framed::new(stream, PacketsCodec::new());

    // Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    let connect: Connect = match packets.next().await {
        Some(Ok(connect)) => connect.try_into()?,
        _ => {
            // We didn't get a CONNECT so we return early here.
            info!("Failed to get CONNECT from {}. Client disconnected.", addr);
            return Ok(());
        }
    };

    let (client, client_id) = Client::new(broker.clone(), connect, packets).await?;

    client.poll().await?;
    info!("Disconnecting...");

    {
        let mut broker = broker.lock().await;
        broker.disconnect(client_id);
    }

    Ok(())
}