use futures::sync::mpsc;
use bytes::Bytes;

type Tx = mpsc::UnboundedSender<Bytes>;

#[derive(Debug)]
pub struct Broker {}

impl Broker {
    pub fn new() -> Broker {
        Broker {}
    }

    pub fn register_client(&self, client_info: String, tx: &Tx) {
        info!("registering {:?}", client_info);
        tx.unbounded_send(Bytes::from(vec![0b0010_0000, 0b0000_0010, 0b0000_0000, 0b0000_0000]))
            .unwrap();
    }
}
