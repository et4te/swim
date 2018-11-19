use std::net::SocketAddr;
use std::fmt;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gossip {
    Join(Uuid, String), //SocketAddr),
    Alive(Uuid),
    Suspect(Uuid),
    Confirm(Uuid),
}

impl fmt::Debug for Gossip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Gossip::Join(uuid, _) =>
                write!(f, "JOIN({})", uuid.to_hyphenated().to_string()),
            Gossip::Alive(uuid) =>
                write!(f, "ALIVE({})", uuid.to_hyphenated().to_string()),
            Gossip::Suspect(uuid) =>
                write!(f, "SUSPECT({})", uuid.to_hyphenated().to_string()),
            Gossip::Confirm(uuid) =>
                write!(f, "CONFIRM({})", uuid.to_string()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Message {
    Join(Uuid, SocketAddr),
    Ping(Uuid, Vec<Gossip>),
    Ack(Uuid, Vec<Gossip>),
    PingReq(Uuid, Uuid),
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Message::Join(uuid, _) =>
                write!(f, "JOIN({})", uuid.to_string()),
            Message::Ping(uuid, gossip) =>
                write!(f, "PING({},{:?})", uuid.to_string(), gossip),
            Message::Ack(uuid, gossip) =>
                write!(f, "ACK({},{:?})", uuid.to_string(), gossip),
            Message::PingReq(peer_uuid, suspect_uuid) =>
                write!(f, "PING-REQ({},{})", peer_uuid.to_string(), suspect_uuid.to_string()),
        }
    }
}
