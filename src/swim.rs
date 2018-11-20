use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Instant, Duration};
use tokio::prelude::*;
use tokio::timer::Interval;
use tokio;
use uuid::Uuid;
use membership::{State, Membership};
use dissemination::Dissemination;
use cache::TimeoutCache;
use client::{self, run_send, spawn_send};
use message::{Message, Gossip};

const PROTOCOL_PERIOD: u64 = 1000;
const ROUND_TRIP_TIME: u64 = 333;

#[derive(Clone)]
pub struct Swim {
    uuid: Uuid,
    addr: SocketAddr,
    membership: Arc<Membership>,
    dissemination: Arc<Dissemination>,
    timeout_cache: Arc<TimeoutCache>,
}

impl Swim {

    pub fn new(addr: SocketAddr) -> Swim {
        Swim {
            uuid: Uuid::new_v4(),
            addr,
            membership: Arc::new(Membership::new()),
            dissemination: Arc::new(Dissemination::new()),
            timeout_cache: Arc::new(TimeoutCache::new()),
        }
    }

    pub fn send_bootstrap_join(&self, seed_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        run_send(seed_addr, message);
    }

    pub fn send_self_join(&self, peer_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        spawn_send(peer_addr, message);
    }

    pub fn send_ping(&self, peer_uuid: Uuid) {
        let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
        let message = Message::Ping(self.uuid, gossip);
        match self.membership.get(peer_uuid.clone()) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();
                let timeout_cache_1 = self.timeout_cache.clone();
                let timeout_cache_2 = self.timeout_cache.clone();
                let membership = self.membership.clone();
                let dissemination = self.dissemination.clone();
                let peer_uuid_1 = peer_uuid.clone();
                let send = client::send(peer_addr.clone(), message)
                    .and_then(move |_| {
                        timeout_cache_1.create_ack_timeout(peer_uuid_1.clone());
                        Ok(())
                    }).map_err(move |err| {
                        println!("[swim] suspecting peer {:?}", peer_uuid.clone());
                        membership.suspect(peer_uuid);
                        dissemination.gossip_suspect(peer_uuid);
                        timeout_cache_2.create_suspect_timeout(peer_uuid);
                    });
                tokio::spawn(send);
            }
            None => {
                println!("[swim] unknown peer_uuid")
            }
        }
    }

    pub fn forward_ping(&self, peer_uuid: Uuid, suspect_uuid: Uuid) {
        println!("[swim] forward ping initiated");
        let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
        let message = Message::Ping(self.uuid, gossip);
        match self.membership.get(peer_uuid) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();

                match self.membership.get(suspect_uuid) {
                    Some(entry) => {
                        let (suspect_addr, _) = entry.value();

                        let timeout_cache = self.timeout_cache.clone();
                        let membership = self.membership.clone();
                        let send = client::send(*peer_addr, message)
                            .and_then(move |_| {
                                Ok(())
                            }).map_err(move |err| {
                                // Suspect suspected peer if connection fails
                            });
                        tokio::spawn(send);
                    }
                    None => {
                        println!("[swim] unknown suspect_uuid")
                    }
                }
            }
            None => {
                println!("[swim] Unknown peer_uuid")
            }
        }
    }

    pub fn send_ack(&self, peer_uuid: Uuid) {
        let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
        let message = Message::Ack(self.uuid, gossip);
        self.membership.send(peer_uuid, message);
    }

    pub fn handle_join(&self, peer_uuid: Uuid, peer_addr: SocketAddr) {
        if self.membership.process_join(peer_uuid, peer_addr) {
            self.send_self_join(peer_addr);
            self.dissemination.gossip_join(peer_uuid, peer_addr);
        } else {
            // println!("[swim] received duplicate join request for {:?}", peer_addr)
        }
    }

    pub fn handle_ping(&self, peer_uuid: Uuid, gossip_vec: Vec<Gossip>) {
        match self.membership.get(peer_uuid) {
            Some(_) => {
                for gossip in gossip_vec {
                    self.process_gossip(gossip);
                }
                self.send_ack(peer_uuid);
            }
            None => {
                println!("[swim] unknown peer");
            }
        }
    }

    pub fn handle_ack(&self, peer_uuid: Uuid, gossip_vec: Vec<Gossip>) {
        match self.membership.get(peer_uuid) {
            Some(entry) => {
                for gossip in gossip_vec {
                    self.process_gossip(gossip);
                }

                let (_, peer_state) = entry.value();
                match peer_state {
                    State::Alive => {
                        // Remove timeout if exists
                        // println!("[swim] removing timeout for {:?}", peer_uuid.clone());
                        self.timeout_cache.remove_ack_timeout(peer_uuid);
                    }
                    State::Suspected => {
                        // Remove timeout if exists
                        println!("[swim] unsuspecting {:?}", peer_uuid.clone());
                        self.membership.alive(peer_uuid);
                        self.dissemination.gossip_alive(peer_uuid);
                        self.timeout_cache.remove_suspect_timeout(peer_uuid);
                        self.timeout_cache.remove_ack_timeout(peer_uuid);
                    }
                }
            }
            None => {
                println!("[swim] unknown peer");
            }
        }
    }

    pub fn handle_ping_req(&self, peer_uuid: Uuid, suspect_uuid: Uuid) {
        match self.membership.get(peer_uuid) {
            Some(entry) => {
                let (peer_addr, _) = entry.value();
                match self.membership.get(suspect_uuid) {
                    Some(entry) => {
                        let (suspect_addr, _) = entry.value();
                        // If this node is a suspect, immediately ack
                        if self.addr == *suspect_addr {
                            self.send_ack(peer_uuid);
                            // gossip alive state
                        } else {
                            // Otherwise initiate a probe
                            self.forward_ping(peer_uuid, suspect_uuid);
                        }
                    }
                    None => {
                        println!("[server] unknown suspect");
                    }
                }
            }
            None => {
                println!("[server] unknown peer");
            }
        }
    }

    pub fn handle_timeouts(&self) {
        if let Ok(Async::Ready(timeouts)) = self.timeout_cache.poll_purge() {
            for expired_uuid in timeouts.ack_uuids {
                let members = self.membership.sample(1, vec![self.uuid.clone()]);
                if members.len() > 0 {
                    let entry = &members[0];
                    let peer_uuid = entry.key();
                    // println!("[swim] sampled peer = {:?}", peer_uuid.clone());
                    let message = Message::PingReq(self.uuid.clone(), expired_uuid);
                    self.membership.send(peer_uuid.clone(), message);
                    self.timeout_cache.create_ack_timeout(expired_uuid);
                }
            }

            for expired_uuid in timeouts.indirect_ack_uuids {
                // Disseminate suspected
                println!("[swim] disseminating suspect = {:?}", expired_uuid);
                self.membership.suspect(expired_uuid);
                self.dissemination.gossip_suspect(expired_uuid);
                self.timeout_cache.create_suspect_timeout(expired_uuid);
            }

            for expired_uuid in timeouts.suspect_uuids {
                // Disseminate confirmed failure
                println!("[swim] disseminating confirmed = {:?}", expired_uuid);
                self.membership.remove(expired_uuid);
                self.dissemination.gossip_confirm(expired_uuid);
            }
        }
    }

    fn process_gossip(&self, gossip: Gossip) {
        // println!("[swim] GOSSIP: {:?}", gossip.clone());
        match gossip.clone() {
            Gossip::Join(peer_uuid, peer_addr_string) => {
                if peer_uuid != self.uuid {
                    let peer_addr = peer_addr_string.parse().unwrap();
                    if self.membership.process_join(peer_uuid, peer_addr) {
                        self.send_self_join(peer_addr);
                        self.dissemination.gossip_join(peer_uuid, peer_addr);
                    }
                }
            }
            // Clear the suspect timeout & mark as alive
            Gossip::Alive(peer_uuid) => {
                self.membership.alive(peer_uuid);
                self.dissemination.gossip_alive(peer_uuid);
                println!("[swim] removing suspect timeout");
                self.timeout_cache.remove_suspect_timeout(peer_uuid);
            }
            // Create a suspect timeout & mark as suspected
            Gossip::Suspect(peer_uuid) => {
                self.membership.suspect(peer_uuid);
                self.dissemination.gossip_suspect(peer_uuid);
                println!("[swim] creating suspect timeout");
                self.timeout_cache.create_suspect_timeout(peer_uuid);
            }
            // Remove the peer from the membership map
            Gossip::Confirm(peer_uuid) => {
                self.membership.remove(peer_uuid);
            }
        }
    }
    
    pub fn run(self) {
        let protocol_period = Duration::from_millis(PROTOCOL_PERIOD);
        let swim = Interval::new(Instant::now(), protocol_period)
            .for_each(move |_instant| {
                // println!("[swim] members = {:?}", self.membership.len());
                if self.membership.len() >= 2 {
                    let members = self.membership.sample(1, vec![self.uuid.clone()]);
                    if members.len() > 0 {
                        let entry = &members[0];
                        let peer_uuid = entry.key();
                        self.send_ping(*peer_uuid);
                    }
                    self.handle_timeouts();
                    Ok(())
                } else {
                    Ok(())
                }
            }).map_err(|err| {
                panic!("[swim] interval error; err = {:?}", err);
            });
        tokio::run(swim);
    }
}
