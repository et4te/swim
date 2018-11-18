
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::collections::HashMap;
use tokio::prelude::*;
use tokio::timer::{self, delay_queue, DelayQueue};
use uuid::Uuid;

const ROUND_TRIP_TIME: u64 = 333;

#[derive(Clone)]
pub struct TimeoutCache {
    ack_map: Arc<Mutex<HashMap<Uuid, delay_queue::Key>>>,
    indirect_ack_map: Arc<Mutex<HashMap<Uuid, delay_queue::Key>>>,
    ack_timeouts: Arc<Mutex<DelayQueue<Uuid>>>,
    indirect_ack_timeouts: Arc<Mutex<DelayQueue<Uuid>>>,
}

impl TimeoutCache {

    pub fn new() -> TimeoutCache {
        TimeoutCache {
            ack_map: Arc::new(Mutex::new(HashMap::new())),
            indirect_ack_map: Arc::new(Mutex::new(HashMap::new())),
            ack_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
            indirect_ack_timeouts: Arc::new(Mutex::new(DelayQueue::new())),
        }
    }

    // When a Ping message is sent to a peer, a key is stored which expires after
    // ROUND_TRIP_TIME has elapsed.
    pub fn create_ack_timeout(&self, peer_uuid: Uuid) {
        let timeout_key = self.ack_timeouts.lock().unwrap()
            .insert(peer_uuid, Duration::from_millis(ROUND_TRIP_TIME));
        self.ack_map.lock().unwrap()
            .insert(peer_uuid, timeout_key);
    }

    // When a PingReq message is sent to a peer a secondary key is stored which
    // expires after ROUND_TRIP_TIME has elapsed.
    pub fn create_indirect_ack_timeout(&self, suspect_uuid: Uuid) {
        let timeout_key = self.indirect_ack_timeouts.lock().unwrap()
            .insert(suspect_uuid, Duration::from_millis(ROUND_TRIP_TIME));
        self.indirect_ack_map.lock().unwrap()
            .insert(suspect_uuid, timeout_key);
    }

    pub fn remove_ack_timeout(&self, peer_uuid: Uuid) {
        let mut ack_map = self.ack_map.lock().unwrap();
        if let Some(expiration_key) = ack_map.remove(&peer_uuid) {
            let mut ack_timeouts = self.ack_timeouts.lock().unwrap();
            ack_timeouts.remove(&expiration_key);
        }
    }

    pub fn remove_indirect_ack_timeout(&self, peer_uuid: Uuid) {
        let mut indirect_ack_map = self.indirect_ack_map.lock().unwrap();
        if let Some(expiration_key) = indirect_ack_map.remove(&peer_uuid) {
            let mut indirect_ack_timeouts = self.indirect_ack_timeouts.lock().unwrap();
            indirect_ack_timeouts.remove(&expiration_key);
        }
    }

    pub fn poll_purge(&self) -> Poll<(Vec<Uuid>, Vec<Uuid>), timer::Error> {
        let mut ack_timeout_uuids = vec![];
        let mut ack_timeouts = self.ack_timeouts.lock().unwrap();
        let mut ack_map = self.ack_map.lock().unwrap();
        while let Some(expired) = try_ready!(ack_timeouts.poll()) {
            let suspect_uuid = expired.get_ref().clone();
            println!("[cache] ack {:?} expired", suspect_uuid);
            let _ = ack_map.remove(&suspect_uuid).unwrap();
            ack_timeout_uuids.push(suspect_uuid);
        }

        let mut indirect_ack_timeout_uuids = vec![];
        let mut indirect_ack_timeouts = self.indirect_ack_timeouts.lock().unwrap();
        let mut indirect_ack_map = self.indirect_ack_map.lock().unwrap();
        while let Some(expired) = try_ready!(indirect_ack_timeouts.poll()) {
            let suspect_uuid = expired.get_ref().clone();
            println!("[cache] indirect_ack {:?} expired", suspect_uuid);
            let _ = indirect_ack_map.remove(&suspect_uuid).unwrap();
            indirect_ack_timeout_uuids.push(suspect_uuid);
        }

        Ok(Async::Ready((ack_timeout_uuids, indirect_ack_timeout_uuids)))
    }
}
