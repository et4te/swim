use std::net::SocketAddr;

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq, Debug)]
pub enum Gossip {
    Alive(SocketAddr),
    Suspect(SocketAddr),
    Confirm(SocketAddr),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Message {
    Ping(SocketAddr, Vec<Gossip>),
    Ack(SocketAddr, Vec<Gossip>),
    PingReq(SocketAddr, SocketAddr),
}
