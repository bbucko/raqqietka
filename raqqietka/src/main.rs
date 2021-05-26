#![warn(rust_2018_idioms)]

use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io;
use tokio::io::AsyncRead;
use tokio::net;
use tokio::sync;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::codec;
use tracing::{debug, error, info, info_span, Level};
use tracing_futures::Instrument;
use tracing_subscriber::{fmt, EnvFilter};

use broker::*;
use core::*;
use mqtt::*;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();

    let bind_addr = "127.0.0.1:1883".parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

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

async fn process(broker: MqttBroker, socket: net::TcpStream) -> Result<(), core::MQTTError> {
    debug!("accepted connection");

    let (read, write) = io::split(socket);

    let mut packets = codec::FramedRead::new(read, PacketsCodec::new());

    let connect = get_first_packet(&mut packets).await?;

    let (client, client_id, will, msg_in, msg_out, commands) = Client::new(connect, broker.clone()).await?;

    let span = info_span!("mqtt", %client_id);

    let (close_tx, close_rx) = mpsc::unbounded_channel();
    let close_rx = UnboundedReceiverStream::new(close_rx);

    tokio::spawn(msg_out.forward_to(write, close_tx).instrument(span.clone()));

    //TODO: add health-check watcher

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();

        broker.register(&client_id, msg_in.clone(), will).and_then(|_| msg_in.ack(Ack::Connect))?;
    }

    let mut packets = packets
        .map(|result| result.map(Command::PACKET))
        .merge(commands.map(MQTTResult::Ok))
        .merge(close_rx.map(MQTTResult::Ok));

    while let Some(packet) = packets.next().await {
        match packet {
            Ok(Command::PACKET(packet)) => {
                client
                    .process(packet)
                    .instrument(span.clone())
                    .await?
                    .and_then(|response| msg_in.ack(response).ok());
            }
            Ok(Command::DISCONNECT) => {
                break;
            }
            Err(e) => {
                error!(reason = %e, "an error occurred while processing messages for {}", client_id);
                // return Err(MQTTError::OtherError(e.to_string()));
                break;
            }
        }
    }

    {
        let mut broker = broker.lock().instrument(span.clone()).await;
        let _guard = span.enter();

        info!("performing cleanup");
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
            error!(%error, "failed to parse CONNECT - client disconnected.");
            Err(core::MQTTError::ClientError("invalid_connect".to_string()))
        }
        None => {
            //Disconnected before sending a CONNECT
            error!("failed to get CONNECT - client disconnected");
            Err(core::MQTTError::ClientError("client_disconnected".to_string()))
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
