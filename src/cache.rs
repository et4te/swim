use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;
use futures::sync::mpsc::UnboundedSender;
use tokio::prelude::*;
use tokio::timer::{self, delay_queue, DelayQueue};
use message::{NetAddr, Response};

const ROUND_TRIP_TIME: u64 = 333;

pub struct Timeouts {
    pub suspect_addr_vec: Vec<NetAddr>,
    pub query_addr_vec: Vec<NetAddr>,
}

#[derive(Clone)]
pub struct TimeoutCache {
    // Tracks nodes suspected of failure
    suspect_map: Arc<Mutex<HashMap<NetAddr, delay_queue::Key>>>,
    suspect_timeouts: Arc<Mutex<DelayQueue<NetAddr>>>,
    // Tracks protocol timeouts
    query_map: Arc<Mutex<HashMap<NetAddr, delay_queue::Key>>>,
    query_timeouts: Arc<Mutex<DelayQueue<NetAddr>>>,
}

impl TimeoutCache {

    pub fn new() -> TimeoutCache {
        TimeoutCache {
            suspect_map: Arc::new(Mutex::new(HashMap::new())),
            suspect_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            query_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
        }
    }

    pub fn create_suspect_timeout(&self, suspect_addr: NetAddr) {
        let mut suspect_timeouts = self.suspect_timeouts.lock().unwrap();
        let mut suspect_map = self.suspect_map.lock().unwrap();
        match suspect_map.get(&suspect_addr) {
            Some(_) =>
                (),
            None => {
                let timeout_key = suspect_timeouts
                    .insert(suspect_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
                suspect_map
                    .insert(suspect_addr, timeout_key);
            }
        }
    }

    pub fn create_query_timeout(&self, peer_addr: NetAddr) {
        let mut query_timeouts = self.query_timeouts.lock().unwrap();
        let mut query_map = self.query_map.lock().unwrap();
        match query_map.get(&peer_addr) {
            Some(_) =>
                (),
            None => {
                let timeout_key = query_timeouts
                    .insert(peer_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
                query_map
                    .insert(peer_addr, timeout_key);
            }
        }
    }
    
    pub fn remove_suspect_timeout(&self, suspect_addr: &NetAddr) {
        let mut suspect_map = self.suspect_map.lock().unwrap();
        let mut suspect_timeouts = self.suspect_timeouts.lock().unwrap();
        if let Some(expiration_key) = suspect_map.remove(suspect_addr) {
            suspect_timeouts.remove(&expiration_key);
        }
    }

    pub fn remove_query_timeout(&self, peer_addr: &NetAddr) {
        let mut query_map = self.query_map.lock().unwrap();
        let mut query_timeouts = self.query_timeouts.lock().unwrap();
        if let Some(expiration_key) = query_map.remove(peer_addr) {
            query_timeouts.remove(&expiration_key);
        }
    }

    pub fn poll_purge(&self) -> Poll<Timeouts, timer::Error> {
        let mut suspect_timeout_addr_vec = vec![];
        let mut suspect_timeouts = self.suspect_timeouts.lock().unwrap();
        let mut suspect_map = self.suspect_map.lock().unwrap();
        while let Some(expired) = try_ready!(suspect_timeouts.poll()) {
            let suspect_addr = expired.get_ref().clone();
            println!("[cache] suspect {:?} expired", suspect_addr);
            let _ = suspect_map.remove(&suspect_addr).unwrap();
            suspect_timeout_addr_vec.push(suspect_addr);
        }

        let mut query_timeout_addr_vec = vec![];
        let mut query_timeouts = self.query_timeouts.lock().unwrap();
        let mut query_map = self.query_map.lock().unwrap();
        while let Some(expired) = try_ready!(query_timeouts.poll()) {
            let peer_addr = expired.get_ref().clone();
            println!("[cache] query sent to {:?} expired", peer_addr);
            let _ = query_map.remove(&peer_addr).unwrap();
            query_timeout_addr_vec.push(peer_addr);
        }

        let timeouts = Timeouts {
            suspect_addr_vec: suspect_timeout_addr_vec,
            query_addr_vec: query_timeout_addr_vec,
        };

        Ok(Async::Ready(timeouts))
    }
}
