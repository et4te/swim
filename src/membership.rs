
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use uuid::Uuid;
use message::Message;
use client::spawn_send;

#[derive(Debug)]
pub enum State {
    Alive,
    Suspected,
    Confirmed,
}

type MembershipMap = SkipMap<Uuid, (SocketAddr, State)>;

#[derive(Clone)]
pub struct Membership {
    inner: Arc<MembershipMap>,
}

impl Membership {

    pub fn new() -> Membership {
        Membership {
            inner: Arc::new(SkipMap::new())
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get(&self, uuid: Uuid) -> Option<Entry<Uuid, (SocketAddr, State)>> {
        self.inner.get(&uuid)
    }

    pub fn process_join(&mut self, peer_uuid: Uuid, peer_addr: SocketAddr) -> bool  {
        match self.inner.get(&peer_uuid) {
            Some(_) =>
                false,
            None => {
                self.inner.insert(peer_uuid, (peer_addr, State::Alive));
                true
            }
        }
    }

    pub fn sample(&self, count: usize, exclude: Vec<Uuid>) -> Vec<Entry<Uuid, (SocketAddr, State)>> {
        let mut members = vec![];
        for entry in self.inner.iter() {
            if !exclude.contains(entry.key()) {
                members.push(entry)
            }
        }
        members
    }

    pub fn send(&self, peer_uuid: Uuid, message: Message) {
        match self.inner.get(&peer_uuid) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();
                spawn_send(*peer_addr, message);
            }
            None => {
                println!("[membership] Unknown peer");
            }
        }
    }

}
