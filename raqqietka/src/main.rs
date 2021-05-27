use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io;
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
use futures_util::TryStreamExt;
use mqtt::*;
use mqtt_proto::packet::VariablePacket;
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

    let decoder = mqtt_proto::packet::MqttCodec::new();

    let mut packets = codec::FramedRead::new(read, decoder);

    let connect = packets.next().await.unwrap().map_err(|err| MQTTError::ClientError(err.to_string()))?;
    if let VariablePacket::ConnectPacket(connect) = connect {
        let (client, client_id, will, msg_in, msg_out, commands) = Client::new(connect, broker.clone()).await?;

        let span = info_span!("mqtt", %client_id);

        let (close_tx, close_rx) = mpsc::unbounded_channel::<Command>();
        let close_rx = UnboundedReceiverStream::new(close_rx);

        tokio::spawn(msg_out.forward_to(write, close_tx).instrument(span.clone()));

        //TODO: add health-check watcher

        {
            let mut broker = broker.lock().instrument(span.clone()).await;
            let _guard = span.enter();

            broker.register(&client_id, msg_in.clone(), will).and_then(|_| msg_in.ack(Ack::Connect))?;
        }

        let mut packets = packets
            .map_err(|err| //FIXME improve error handling
                MQTTError::ClientError("invalid_packet".to_string()))
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
                }
            }
        }

        {
            let mut broker = broker.lock().instrument(span.clone()).await;
            let _guard = span.enter();

            info!("performing cleanup");
            broker.cleanup(&client_id);
        }
    }

    Ok(())
}

fn init_logging() {
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(Level::INFO)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}
