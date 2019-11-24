#![warn(rust_2018_idioms)]

use std::convert::TryInto;
use std::sync::Arc;

use tokio::codec::FramedRead;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::Mutex;
use tracing::{debug, info, info_span, Level};
use tracing_futures::Instrument;
use tracing_subscriber::fmt;

use broker::Broker;
use core::Connect;
use core::Publisher;
use mqtt::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = fmt::Subscriber::builder().with_max_level(Level::INFO).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let bind_addr = "127.0.0.1:1883";
    let mut listener = TcpListener::bind(&bind_addr).await?;

    info!(addr = bind_addr, "listening on");

    let broker = Arc::new(Mutex::new(Broker::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        let broker = Arc::clone(&broker);

        let future = async move {
            if let Err(e) = process(broker, socket).await {
                println!("an error occurred: {:?}", e);
            }
        };

        tokio::spawn(future.instrument(tracing::trace_span!("conn", ip = %addr.ip(), port = addr.port())));
    }
}

async fn process(broker: Arc<Mutex<Broker>>, connection: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    debug!("accepted connection");

    let (read, write) = io::split(connection);
    let mut packets = FramedRead::new(read, mqtt::PacketsCodec::default());

    // Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    let connect: Connect = match packets.next().await {
        Some(Ok(connect)) => connect.try_into()?,
        Some(Err(error)) => {
            // We didn't get a CONNECT so we return early here.
            info!(%error, "failed to get parse CONNECT - client disconnected.");
            return Ok(());
        }
        None => {
            info!("client disconnected before sending a CONNECT");
            return Ok(());
        }
    };

    let (client, tx_publisher, rx_publisher, client_id) = Client::new(connect).await?;

    broker.lock().await.register(client_id.as_str(), Box::new(tx_publisher.clone()))?;

    tokio::spawn(rx_publisher.forward_to(write).instrument(info_span!("mqtt", client_id = %client_id)));

    while let Some(packet) = packets.next().await {
        if let Some(response) = client
            .process_packet(broker.clone(), packet)
            .instrument(info_span!("mqtt", client_id = %client_id))
            .await?
        {
            tx_publisher.send(response)?;
        }
    }

    broker.lock().await.disconnect(client_id);

    Ok(())
}
