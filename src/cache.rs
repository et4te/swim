use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;
use tokio::prelude::*;
use tokio::timer::{self, delay_queue, DelayQueue};
use message::NetAddr;

const ROUND_TRIP_TIME: u64 = 333;

pub struct Timeouts {
    pub ack_addr_vec: Vec<NetAddr>,
    pub indirect_ack_addr_vec: Vec<NetAddr>,
    pub probe_addr_vec: Vec<NetAddr>,
    pub suspect_addr_vec: Vec<NetAddr>,
    pub query_addr_vec: Vec<NetAddr>,
}

#[derive(Clone)]
pub struct TimeoutCache {
    // Tracks acks received from pinged peers
    ack_map: Arc<Mutex<HashMap<NetAddr, delay_queue::Key>>>,
    ack_timeouts: Arc<Mutex<DelayQueue<NetAddr>>>,
    // Tracks acks received from peers requested by other peers
    indirect_ack_map: Arc<Mutex<HashMap<NetAddr, (NetAddr, delay_queue::Key)>>>,
    indirect_ack_timeouts: Arc<Mutex<DelayQueue<NetAddr>>>,
    // Tracks acks received back from probes
    probe_map: Arc<Mutex<HashMap<NetAddr, delay_queue::Key>>>,
    probe_timeouts: Arc<Mutex<DelayQueue<NetAddr>>>,
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
            ack_map: Arc::new(Mutex::new(HashMap::new())),
            ack_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            indirect_ack_map: Arc::new(Mutex::new(HashMap::new())),
            indirect_ack_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            probe_map: Arc::new(Mutex::new(HashMap::new())),
            probe_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            suspect_map: Arc::new(Mutex::new(HashMap::new())),
            suspect_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            query_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
        }
    }

    // When a Ping message is sent to a peer, a key is stored which expires after
    // ROUND_TRIP_TIME has elapsed.
    pub fn create_ack_timeout(&self, peer_addr: NetAddr) {
        let mut ack_timeouts = self.ack_timeouts.lock().unwrap();
        let mut ack_map = self.ack_map.lock().unwrap();
        match ack_map.get(&peer_addr) {
            Some(_) => // allow only one ack_timeout per peer_addr
                (),
            None => {
                let timeout_key = ack_timeouts
                    .insert(peer_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
                ack_map
                    .insert(peer_addr, timeout_key);
            }
        }
    }

    // When a PingReq message is received by a peer, the peer tries to reach the
    // target. If this request is successful, the ack is forwarded to the peer
    // requesting the probe.
    pub fn create_indirect_ack_timeout(&self, suspect_addr: NetAddr, peer_addr: NetAddr) {
        let mut indirect_ack_timeouts = self.indirect_ack_timeouts.lock().unwrap();
        let mut indirect_ack_map = self.indirect_ack_map.lock().unwrap();
        match indirect_ack_map.get(&suspect_addr) {
            Some(_) => // allow only one indirect_ack_timeout per peer
                (),
            None => {
                let timeout_key = indirect_ack_timeouts
                    .insert(suspect_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
                indirect_ack_map
                    .insert(suspect_addr, (peer_addr, timeout_key));
            }
        }
    }

    // When a PingReq message is sent to a peer this is known as a probe. If
    // the probe timeout expires it indicates that the peer was unsuccessful
    // in reaching the target, thus the target becomes a suspect.
    pub fn create_probe_timeout(&self, suspect_addr: NetAddr) {
        let mut probe_timeouts = self.probe_timeouts.lock().unwrap();
        let mut probe_map = self.probe_map.lock().unwrap();
        match probe_map.get(&suspect_addr) {
            Some(_) =>
                (),
            None => {
                let timeout_key = probe_timeouts
                    .insert(suspect_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
                probe_map
                    .insert(suspect_addr, timeout_key);
            }
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
    
    pub fn remove_ack_timeout(&self, peer_addr: &NetAddr) {
        let mut ack_map = self.ack_map.lock().unwrap();
        let mut ack_timeouts = self.ack_timeouts.lock().unwrap();
        if let Some(expiration_key) = ack_map.remove(peer_addr) {
            ack_timeouts.remove(&expiration_key);
        }
    }

    pub fn remove_indirect_ack_timeout(&self, suspect_addr: &NetAddr) -> Option<NetAddr> {
        let mut indirect_ack_map = self.indirect_ack_map.lock().unwrap();
        if let Some((peer_addr, expiration_key)) = indirect_ack_map.remove(suspect_addr) {
            let mut indirect_ack_timeouts = self.indirect_ack_timeouts.lock().unwrap();
            indirect_ack_timeouts.remove(&expiration_key);
            Some(peer_addr)
        } else {
            None
        }
    }

    pub fn remove_probe_timeout(&self, peer_addr: &NetAddr) {
        let mut probe_map = self.probe_map.lock().unwrap();
        if let Some(expiration_key) = probe_map.remove(peer_addr) {
            let mut probe_timeouts = self.probe_timeouts.lock().unwrap();
            probe_timeouts.remove(&expiration_key);
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
        let mut ack_timeout_addr_vec = vec![];
        let mut ack_timeouts = self.ack_timeouts.lock().unwrap();
        let mut ack_map = self.ack_map.lock().unwrap();
        while let Some(expired) = try_ready!(ack_timeouts.poll()) {
            let suspect_addr = expired.get_ref().clone();
            println!("[cache] ack {:?} expired", suspect_addr);
            let _ = ack_map.remove(&suspect_addr).unwrap();
            ack_timeout_addr_vec.push(suspect_addr);
        }

        let mut indirect_ack_timeout_addr_vec = vec![];
        let mut indirect_ack_timeouts = self.indirect_ack_timeouts.lock().unwrap();
        let mut indirect_ack_map = self.indirect_ack_map.lock().unwrap();
        while let Some(expired) = try_ready!(indirect_ack_timeouts.poll()) {
            let suspect_addr = expired.get_ref().clone();
            println!("[cache] indirect_ack {:?} expired", suspect_addr);
            let _ = indirect_ack_map.remove(&suspect_addr).unwrap();
            indirect_ack_timeout_addr_vec.push(suspect_addr);
        }

        let mut probe_timeout_addr_vec = vec![];
        let mut probe_timeouts = self.probe_timeouts.lock().unwrap();
        let mut probe_map = self.probe_map.lock().unwrap();
        while let Some(expired) = try_ready!(probe_timeouts.poll()) {
            let suspect_addr = expired.get_ref().clone();
            println!("[cache] probe {:?} expired", suspect_addr);
            let _ = probe_map.remove(&suspect_addr).unwrap();
            probe_timeout_addr_vec.push(suspect_addr);
        }

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
            ack_addr_vec: ack_timeout_addr_vec,
            indirect_ack_addr_vec: indirect_ack_timeout_addr_vec,
            probe_addr_vec: probe_timeout_addr_vec,
            suspect_addr_vec: suspect_timeout_addr_vec,
            query_addr_vec: query_timeout_addr_vec,
        };

        Ok(Async::Ready(timeouts))
    }
}
