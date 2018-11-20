use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashSet;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use uuid::Uuid;
use rand::{self, Rng};
use message::Message;
use client::spawn_send;

#[derive(Debug)]
pub enum State {
    Alive,
    Suspected,
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

    pub fn remove(&self, uuid: Uuid) {
        match self.get(uuid) {
            Some(_) => {
                self.inner.remove(&uuid).unwrap();
            }
            None =>
                println!("[membership] attempt to remove non-existent entry"),
        }
    }

    pub fn alive(&self, uuid: Uuid) {
        match self.get(uuid) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();
                self.inner.insert(uuid, (peer_addr.clone(), State::Alive));
            }
            None => ()
        }
    }

    pub fn suspect(&self, uuid: Uuid) {
        match self.get(uuid) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();
                self.inner.insert(uuid, (peer_addr.clone(), State::Suspected));
            }
            None => ()
        }
    }

    pub fn process_join(&self, peer_uuid: Uuid, peer_addr: SocketAddr) -> bool  {
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
        assert!(count < self.inner.len());

        let mut members = vec![];
        for entry in self.inner.iter() {
            if !exclude.contains(entry.key()) {
                members.push(entry)
            }
        }

        // Pick count indices at random below self.inner.len()
        let mut rng = rand::thread_rng();
        let mut indices: HashSet<usize> = HashSet::new();
        while indices.len() < count {
            let r: usize = rng.gen_range(0, self.inner.len());
            indices.insert(r);
        }

        let mut sample = vec![];
        for i in indices.iter().cloned() {
            sample.push(members[i].clone());
        }
        sample
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
