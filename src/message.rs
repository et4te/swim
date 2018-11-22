use std::net::SocketAddr;
use std::fmt;
use slush::Colour;

/// A wrapper type for SocketAddr with usable derivations.

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct NetAddr {
    inner: String,
}

impl NetAddr {
    pub fn new(addr: SocketAddr) -> NetAddr {
        NetAddr { inner: addr.to_string() }
    }

    pub fn to_socket_addr(&self) -> SocketAddr {
        self.inner.parse().unwrap()
    }
}

impl fmt::Debug for NetAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gossip {
    Join(NetAddr),
    Alive(NetAddr),
    Suspect(NetAddr),
    Confirm(NetAddr),
}

impl fmt::Debug for Gossip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Gossip::Join(addr) =>
                write!(f, "JOIN({:?})", addr),
            Gossip::Alive(addr) =>
                write!(f, "ALIVE({:?})", addr),
            Gossip::Suspect(addr) =>
                write!(f, "SUSPECT({:?})", addr),
            Gossip::Confirm(addr) =>
                write!(f, "CONFIRM({:?})", addr),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Request {
    Join(NetAddr),
    Ping(NetAddr, Vec<Gossip>),
    PingReq(NetAddr, NetAddr),
    Query(NetAddr, Colour),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Response {
    Join(NetAddr),
    Ack(Vec<Gossip>),
    Respond(Colour),
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Request::Join(addr) =>
                write!(f, "JOIN({:?})", addr),
            Request::Ping(addr, gossip) =>
                write!(f, "PING({:?},{:?})", addr, gossip),
            Request::PingReq(peer_addr, suspect_addr) =>
                write!(f, "PING-REQ({:?},{:?})", peer_addr, suspect_addr),
            Request::Query(peer_addr, col) =>
                write!(f, "QUERY({:?},{:?})", peer_addr, col),
        }
    }
}

impl fmt::Debug for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Join(addr) =>
                write!(f, "JOIN({:?})", addr),
            Response::Ack(gossip) =>
                write!(f, "ACK({:?})", gossip),
            Response::Respond(col) =>
                write!(f, "RESPOND({:?})", col),
        }
    }
}
