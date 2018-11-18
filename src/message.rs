use std::net::SocketAddr;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub enum Gossip {
    Join(SocketAddr),
    Alive(SocketAddr),
    Suspect(SocketAddr),
    Confirm(SocketAddr),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Message {
    Join(Uuid, SocketAddr),
    Ping(Uuid, Vec<Gossip>),
    Ack(Uuid, Vec<Gossip>),
    PingReq(Uuid, Uuid),
}
