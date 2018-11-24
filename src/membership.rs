use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use crossbeam::deque::{self, Worker, Stealer, Steal};
use rand::{self, Rng};
use types::NetAddr;

#[derive(Debug)]
pub enum State {
    Alive,
    Suspected,
}

type MembershipMap = SkipMap<NetAddr, State>;

#[derive(Clone)]
pub struct Membership {
    elements: Arc<MembershipMap>,
    swim_ordering_worker: Arc<Mutex<Worker<NetAddr>>>,
    swim_ordering_stealer: Arc<Stealer<NetAddr>>,
    proto_ordering_worker: Arc<Mutex<Worker<NetAddr>>>,
    proto_ordering_stealer: Arc<Stealer<NetAddr>>,
}

impl Membership {

    pub fn new() -> Membership {
        let (swim_worker, swim_stealer) = deque::lifo::<NetAddr>();
        let (proto_worker, proto_stealer) = deque::lifo::<NetAddr>();
        Membership {
            elements: Arc::new(SkipMap::new()),
            swim_ordering_worker: Arc::new(Mutex::new(swim_worker)),
            swim_ordering_stealer: Arc::new(swim_stealer),
            proto_ordering_worker: Arc::new(Mutex::new(proto_worker)),
            proto_ordering_stealer: Arc::new(proto_stealer),
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn get(&self, addr: &NetAddr) -> Option<Entry<NetAddr, State>> {
        self.elements.get(addr)
    }

    pub fn remove(&self, addr: &NetAddr) {
        match self.get(addr) {
            Some(_) => {
                self.elements.remove(addr).unwrap();
            }
            None =>
                warn!("attempt to remove non-existent entry"),
        }
    }

    pub fn alive(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                info!("setting state {:?} to alive", state);
                self.elements.insert(addr, State::Alive);
            }
            None => ()
        }
    }

    pub fn suspect(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                info!("setting state {:?} to suspect", state);
                self.elements.insert(addr, State::Suspected);
            }
            None => ()
        }
    }

    pub fn process_join(&self, peer_addr: NetAddr) -> bool {
        match self.get(&peer_addr) {
            Some(_) =>
                false,
            None => {
                self.elements.insert(peer_addr, State::Alive);
                true
            }
        }
    }

    // TODO Improve sampling functions

    pub fn sample_rr(&self, count: usize, exclude: Vec<NetAddr>) -> Vec<NetAddr> {
        assert!(count < self.elements.len());

        // drain count keys from the current ordering
        let mut addrs = vec![];
        while addrs.len() < count {
            if let Steal::Data(addr) = self.swim_ordering_stealer.steal() {
                addrs.push(addr)
            } else {
                // Regenerate ordering
                let mut members = vec![];
                for entry in self.elements.iter() {
                    if !exclude.contains(entry.key()) {
                        members.push(entry)
                    }
                }

                // Pick random indices linearly (slow)
                let mut rng = rand::thread_rng();
                let mut indices: HashSet<usize> = HashSet::new();
                while indices.len() < self.elements.len() {
                    let r: usize = rng.gen_range(0, self.elements.len());
                    indices.insert(r);
                }

                for i in indices.iter().cloned() {
                    let addr = members[i].key();
                    self.swim_ordering_worker.lock().unwrap()
                        .push(addr.clone());
                }
            }
        }
        addrs
    }

    pub fn sample(&self, count: usize, exclude: Vec<NetAddr>) -> Vec<NetAddr> {
        assert!(count <= self.elements.len());

        // drain count keys from the current ordering
        let mut addrs = vec![];
        while addrs.len() < count {
            if let Steal::Data(addr) = self.proto_ordering_stealer.steal() {
                addrs.push(addr)
            } else {
                // Regenerate ordering
                let mut members = vec![];
                for entry in self.elements.iter() {
                    if !exclude.contains(entry.key()) {
                        members.push(entry)
                    }
                }

                // Pick random indices linearly (slow)
                let mut rng = rand::thread_rng();
                let mut indices: HashSet<usize> = HashSet::new();
                while indices.len() < self.elements.len() {
                    let r: usize = rng.gen_range(0, self.elements.len());
                    indices.insert(r);
                }

                for i in indices.iter().cloned() {
                    let addr = members[i].key();
                    self.proto_ordering_worker.lock().unwrap()
                        .push(addr.clone());
                }
            }
        }
        addrs
    }
}
