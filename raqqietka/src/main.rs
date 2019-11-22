#![warn(rust_2018_idioms)]

use std::convert::TryInto;
use std::sync::Arc;

use tokio::codec::Framed;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::Mutex;
use tracing::{Level, span};
use tracing::info;

use broker::Broker;
use core::Connect;
use mqtt::{Client, PacketsCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .init();

    let bind_addr = "127.0.0.1:1883";
    let mut listener = TcpListener::bind(&bind_addr).await?;
    info!("raqqietka starting on {}", bind_addr);

    let broker = Arc::new(Mutex::new(Broker::new()));

    loop {
        let (stream, addr) = listener.accept().await?;
        let broker = Arc::clone(&broker);

        tokio::spawn(async move {
            let span = span!(Level::INFO, "tcp", client_addr = addr.to_string().as_str());
            let _enter = span.enter();

            if let Err(e) = process(broker, stream).await {
                println!("an error occurred: {:?}", e);
            }
        });
    }
}

async fn process(broker: Arc<Mutex<Broker>>, stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut packets = Framed::new(stream, PacketsCodec::new());

    // Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    let connect: Connect = match packets.next().await {
        Some(Ok(connect)) => connect.try_into()?,
        _ => {
            // We didn't get a CONNECT so we return early here.
            info!("Failed to get parse CONNECT - client disconnected.");
            return Ok(());
        }
    };

    let (client, client_id) = Client::new(broker.clone(), connect, packets).await?;
    let span = span!(Level::INFO, "mqtt", client_id = client_id.as_str());
    let _enter = span.enter();


    client.poll().await?;

    info!("disconnecting");

    let mut broker = broker.lock().await;
    broker.disconnect(client_id);


    Ok(())
}