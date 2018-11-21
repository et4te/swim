use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashSet;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use rand::{self, Rng};
use message::{NetAddr, Request};
use client::spawn_send;

#[derive(Debug)]
pub enum State {
    Alive,
    Suspected,
}

type MembershipMap = SkipMap<NetAddr, State>;

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

    pub fn get(&self, addr: &NetAddr) -> Option<Entry<NetAddr, State>> {
        self.inner.get(addr)
    }

    pub fn remove(&self, addr: &NetAddr) {
        match self.get(addr) {
            Some(_) => {
                self.inner.remove(addr).unwrap();
            }
            None =>
                println!("[membership] attempt to remove non-existent entry"),
        }
    }

    pub fn alive(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                println!("[membership] overwriting {:?} with alive", state);
                self.inner.insert(addr, State::Alive);
            }
            None => ()
        }
    }

    pub fn suspect(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                println!("[membership] overwriting {:?} with suspect", state);
                self.inner.insert(addr, State::Suspected);
            }
            None => ()
        }
    }

    pub fn process_join(&self, peer_addr: NetAddr) -> bool {
        match self.get(&peer_addr) {
            Some(_) =>
                false,
            None => {
                self.inner.insert(peer_addr, State::Alive);
                true
            }
        }
    }

    pub fn sample(&self, count: usize, exclude: Vec<NetAddr>) -> Vec<Entry<NetAddr, State>> {
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

    pub fn send(&self, peer_addr: NetAddr, request: Request) {
        match self.inner.get(&peer_addr) {
            Some(entry) => {
                spawn_send(peer_addr.to_socket_addr(), request);
            }
            None => {
                println!("[membership] Unknown peer");
            }
        }
    }
}
