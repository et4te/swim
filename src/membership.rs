use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use crossbeam::deque::{self, Worker, Stealer, Steal};
use rand::{self, Rng};
use message::{NetAddr, Request};

#[derive(Debug)]
pub enum State {
    Alive,
    Suspected,
}

type MembershipMap = SkipMap<NetAddr, State>;

#[derive(Clone)]
pub struct Membership {
    elements: Arc<MembershipMap>,
    ordering_worker: Arc<Mutex<Worker<NetAddr>>>,
    ordering_stealer: Arc<Stealer<NetAddr>>,
}

impl Membership {

    pub fn new() -> Membership {
        let (worker, stealer) = deque::lifo::<NetAddr>();
        Membership {
            elements: Arc::new(SkipMap::new()),
            ordering_worker: Arc::new(Mutex::new(worker)),
            ordering_stealer: Arc::new(stealer),
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
                println!("[membership] attempt to remove non-existent entry"),
        }
    }

    pub fn alive(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                println!("[membership] overwriting {:?} with alive", state);
                self.elements.insert(addr, State::Alive);
            }
            None => ()
        }
    }

    pub fn suspect(&self, addr: NetAddr) {
        match self.get(&addr) {
            Some(entry) => {
                let state = entry.value();
                println!("[membership] overwriting {:?} with suspect", state);
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

    pub fn sample_rr(&self, count: usize, exclude: Vec<NetAddr>) -> Vec<NetAddr> {
        assert!(count < self.elements.len());

        // drain count keys from the current ordering
        let mut addrs = vec![];
        while addrs.len() < count {
            if let Steal::Data(addr) = self.ordering_stealer.steal() {
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
                    self.ordering_worker.lock().unwrap()
                        .push(addr.clone());
                }
            }
        }
        addrs
    }

    pub fn sample(&self, count: usize, exclude: Vec<NetAddr>) -> Vec<Entry<NetAddr, State>> {
        assert!(count < self.elements.len());

        let mut members = vec![];
        for entry in self.elements.iter() {
            if !exclude.contains(entry.key()) {
                members.push(entry)
            }
        }

        // Pick count indices at random below self.elements.len()
        let mut rng = rand::thread_rng();
        let mut indices: HashSet<usize> = HashSet::new();
        while indices.len() < count {
            let r: usize = rng.gen_range(0, self.elements.len());
            indices.insert(r);
        }

        let mut sample = vec![];
        for i in indices.iter().cloned() {
            sample.push(members[i].clone());
        }
        sample
    }
}
