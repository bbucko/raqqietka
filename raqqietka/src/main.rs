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
use tracing_subscriber::{fmt, EnvFilter};

use broker::*;
use core::MQTTError::ClientError;
use core::*;
use mqtt::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let bind_addr = "127.0.0.1:1883".parse::<SocketAddr>()?;
    let mut listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    info!(%bind_addr, "listening on");

    let broker = Arc::new(sync::Mutex::new(Broker::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        let broker = broker.clone();

        tokio::spawn(async move {
            let span = tracing::trace_span!("conn", ip = %addr.ip(), port = addr.port());
            if let Err(e) = process(broker, socket).instrument(span).await {
                error!(%e, "an error occurred");
            }
        });
    }
}

async fn process(broker: MqttBroker, socket: net::TcpStream) -> Result<(), MQTTError> {
    debug!("accepted connection");

    let (read, write) = io::split(socket);

    let mut packets = codec::FramedRead::new(read, PacketsCodec::new());

    let connect = get_first_packet(&mut packets).await?;

    let (client, client_id, will, consumer, producer) = Client::new(connect, broker.clone()).await?;
    let span = info_span!("mqtt", %client_id);

    tokio::spawn(producer.forward_to(write).instrument(span.clone()));

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();

        let _ = broker.register(&client_id, consumer.clone(), will).map(|_| consumer.ack(Ack::Connect))?;
    }

    while let Some(packet) = packets.next().await {
        client
            .process(packet)
            .instrument(span.clone())
            .await?
            .and_then(|response| consumer.ack(response).ok());
    }

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();
        broker.cleanup(&client_id);
    }

    Ok(())
}

async fn get_first_packet<R: AsyncRead + Unpin>(packets: &mut codec::FramedRead<R, PacketsCodec>) -> MQTTResult<Connect> {
    //Read the first packet from the `PacketsCodec` stream to get the CONNECT.
    match packets.next().await {
        Some(Ok(connect)) => {
            //Successful connection
            Ok(connect.try_into()?)
        }
        Some(Err(error)) => {
            //Didn't get a CONNECT
            error!(%error, "failed to get parse CONNECT - client disconnected.");
            Err(ClientError("invalid_connect".to_string()))
        }
        None => {
            //Disconnected before sending a CONNECT
            error!("client disconnected before sending a CONNECT");
            Err(ClientError("invalid_connect".to_string()))
        }
    }
}

fn init_logging() {
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(Level::INFO)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}
