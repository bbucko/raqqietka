#![warn(rust_2018_idioms)]

use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::stream::StreamExt;
use tokio::io;
use tokio::net;
use tokio::prelude::*;
use tokio::sync;
use tokio_util::codec;
use tracing::{debug, error, info, info_span, Level};
use tracing_futures::Instrument;
use tracing_subscriber::fmt;

use broker::*;
use core::*;
use core::MQTTError::ClientError;
use mqtt::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let bind_addr = "127.0.0.1:1883".parse::<SocketAddr>()?;
    let mut listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    info!(%bind_addr, "listening on");

    let broker = Arc::new(tokio::sync::Mutex::new(Broker::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        let broker = Arc::clone(&broker);

        tokio::spawn(async move {
            let span = tracing::trace_span!("conn", ip = %addr.ip(), port = addr.port());
            if let Err(e) = process(broker.clone(), socket).instrument(span).await {
                error!(%e, "an error occurred");
            }
        });
    }
}

fn init_logging() {
    let subscriber = fmt::Subscriber::builder().with_max_level(Level::INFO).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

async fn process(broker: Arc<sync::Mutex<Broker<MessageConsumer>>>, connection: net::TcpStream) -> Result<(), MQTTError> {
    debug!("accepted connection");

    let (read, write) = io::split(connection);
    let mut packets = codec::FramedRead::new(read, PacketsCodec::new());
    let connect = first_packet(&mut packets).await?;

    let (client, client_id, will, consumer, producer) = Client::new(connect, broker.clone()).await?;
    let span = info_span!("mqtt", %client_id);

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();

        let _ = broker
            .register(&client_id, consumer.clone(), will)
            .map(|_| consumer.send(core::ConnAck::default().into()))?;
    }

    tokio::spawn(producer.forward_to(write).instrument(span.clone()));

    while let Some(packet) = packets.next().await {
        client
            .process(packet)
            .instrument(span.clone())
            .await?
            .and_then(|response| consumer.send(response).ok());
    }

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();
        broker.disconnect(client_id);
    }

    Ok(())
}

async fn first_packet<R: AsyncRead + Unpin>(packets: &mut codec::FramedRead<R, PacketsCodec>) -> MQTTResult<Connect> {
    //Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    match packets.next().await {
        Some(Ok(connect)) => {
            //Successful connection
            return Ok(connect.try_into()?);
        }
        Some(Err(error)) => {
            //Didn't get a CONNECT
            error!(%error, "failed to get parse CONNECT - client disconnected.");
            return Err(ClientError("invalid_connect".to_string()));
        }
        None => {
            //Disconnected before sending a CONNECT
            error!("client disconnected before sending a CONNECT");
            return Err(ClientError("invalid_connect".to_string()));
        }
    };
}
