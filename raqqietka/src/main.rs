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

use broker::*;
use core::*;
use core::MQTTError::ClientError;
use mqtt::*;

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
    let mut packets = FramedRead::new(read, PacketsCodec::new());
    let connect = first_packet(&mut packets).await?;

    let (client, client_id, consumer, producer) = Client::new(connect).await?;
    let span = info_span!("mqtt", client_id = %client_id);

    {
        let mut broker = broker.lock().await;
        broker.register(client_id.as_str(), Box::new(consumer.clone()))?;
    }

    tokio::spawn(producer.forward_to(write).instrument(span.clone()));

    while let Some(packet) = packets.next().await {
        let processed_packet = client
            .process_packet(broker.clone(), packet)
            .instrument(span.clone())
            .await?;

        if let Some(response) = processed_packet {
            consumer.send(response)?;
        }
    }

    {
        let mut broker = broker.lock().await;
        broker.disconnect(client_id);
    }


    Ok(())
}

async fn first_packet<R: AsyncRead + Unpin>(packets: &mut FramedRead<R, PacketsCodec>) -> MQTTResult<Connect> {
    //Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    let connect: Connect = match packets.next().await {
        Some(Ok(connect)) => connect.try_into()?,
        Some(Err(error)) => {
            // We didn't get a CONNECT so we return early here.
            info!(%error, "failed to get parse CONNECT - client disconnected.");
            return Err(ClientError("invalid_connect".to_string()));
        }
        None => {
            info!("client disconnected before sending a CONNECT");
            return Err(ClientError("invalid_connect".to_string()));
        }
    };
    Ok(connect)
}
