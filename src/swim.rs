use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use std::thread;
use futures::sync::mpsc::UnboundedSender;
use tokio::prelude::*;
use tokio::timer::Interval;
use tokio;
use membership::{State, Membership};
use dissemination::Dissemination;
use cache::TimeoutCache;
use client::{self, run_send, run_request, spawn_send};
use message::{NetAddr, Request, Response, Gossip};
use slush::{Colour, Slush};
use colored::Colorize;

const PROTOCOL_PERIOD: u64 = 1000;
const ROUND_TRIP_TIME: u64 = 333;
const K: usize = 2;

#[derive(Clone)]
pub struct Swim {
    addr: NetAddr,
    delay: Option<u64>,
    membership: Arc<Membership>,
    dissemination: Arc<Dissemination>,
    timeout_cache: Arc<TimeoutCache>,
    protocol_state: Arc<Mutex<Slush>>,
}

impl Swim {

    pub fn new(addr: SocketAddr, delay: Option<u64>) -> Swim {
        Swim {
            addr: NetAddr::new(addr),
            delay,
            membership: Arc::new(Membership::new()),
            dissemination: Arc::new(Dissemination::new()),
            timeout_cache: Arc::new(TimeoutCache::new()),
            protocol_state: Arc::new(Mutex::new(Slush::new())),
        }
    }

    pub fn send_bootstrap_join(&self, seed_addr: SocketAddr) {
        let membership = self.membership.clone();
        let dissemination = self.dissemination.clone();
        let timeout = Duration::from_millis(ROUND_TRIP_TIME);
        let message = Request::Join(self.addr.clone());
        let request = client::request(seed_addr, message, timeout)
            .and_then(move |message| {
                if let Response::Join(peer_addr) = message {
                    if membership.process_join(peer_addr.clone()) {
                        dissemination.gossip_join(peer_addr)
                    }
                } // else error
                Ok(())
            })
            .map_err(|err| {
                println!("[client] bootstrap error = {:?}", err)
            });
        tokio::run(request);
    }

    pub fn send_self_join(&self, peer_addr: SocketAddr) {
        let message = Request::Join(self.addr.clone());
        spawn_send(peer_addr, message);
    }

    pub fn send_ping(&self, peer_addr: NetAddr) {
        let membership = self.membership.clone();
        let dissemination = self.dissemination.clone();
        let timeout_cache = self.timeout_cache.clone();
        let timeout = Duration::from_millis(ROUND_TRIP_TIME);
        let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
        let message = Request::Ping(self.addr.clone(), gossip);
        let request = client::request(peer_addr.to_socket_addr().clone(), message, timeout)
            .and_then(move |message| {
                if let Response::Ack(peer_addr, gossip) = message {
                    ()
                } // else error
                Ok(())
            })
            .map_err(move |err| {
                println!("[client] ping error = {:?}", err);
                membership.suspect(peer_addr.clone());
                dissemination.gossip_suspect(peer_addr.clone());
                timeout_cache.create_suspect_timeout(peer_addr);
            });
        tokio::spawn(request);
    }

    // A peer requests a probe into a suspect
    pub fn forward_ping(&self, peer_addr: NetAddr, suspect_addr: NetAddr) {
        println!("[swim] forward ping initiated");
        let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
        let message = Request::Ping(self.addr.clone(), gossip);
        let timeout_cache = self.timeout_cache.clone();
        let membership = self.membership.clone();
        let send = client::send(suspect_addr.to_socket_addr(), message)
            .and_then(move |_| {
                timeout_cache.create_indirect_ack_timeout(suspect_addr, peer_addr);
                Ok(())
            }).map_err(move |err| {
                // ignore error here
            });
        tokio::spawn(send);
    }

    // pub fn send_ack(&self, peer_addr: NetAddr) {
    //     let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
    //     let message = Response::Ack(self.addr.clone(), gossip);
    //     self.membership.send(peer_addr, message);
    // }

    // pub fn forward_ack(&self, peer_addr: NetAddr, suspect_addr: NetAddr) {
    //     let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
    //     let message = Response::Ack(suspect_addr.clone(), gossip);
    //     self.membership.send(peer_addr, message);
    // }

    //------------------------------------------------------------------------------
    // Protocol
    //------------------------------------------------------------------------------
    pub fn send_query(&self, peer_addr: NetAddr, col: Colour) {
        let message = Request::Query(self.addr.clone(), col);
        let membership = self.membership.clone();
        let dissemination = self.dissemination.clone();
        let timeout_cache_1 = self.timeout_cache.clone();
        let peer_addr_2 = peer_addr.clone();
        let timeout_cache_2 = self.timeout_cache.clone();
        let send = client::send(peer_addr.to_socket_addr().clone(), message)
            .and_then(move |_| {
                timeout_cache_1.create_query_timeout(peer_addr.clone());
                Ok(())
            }).map_err(move |err| {
                println!("[swim] suspecting peer {:?}", peer_addr_2.clone());
                membership.suspect(peer_addr_2.clone());
                dissemination.gossip_suspect(peer_addr_2.clone());
                timeout_cache_2.create_suspect_timeout(peer_addr_2);
            });
        tokio::spawn(send);
    }

    // pub fn send_respond(&self, peer_addr: NetAddr, col: Colour) {
    //     let message = Response::Respond(self.addr.clone(), col);
    //     self.membership.send(peer_addr, message);
    // }
    //------------------------------------------------------------------------------

    pub fn handle_join(&self, sender: UnboundedSender<Response>, peer_addr: NetAddr) {
        if self.membership.process_join(peer_addr.clone()) {
            let message = Response::Join(self.addr.clone());
            let _ = sender.unbounded_send(message).unwrap();
            // self.send_self_join(peer_addr.to_socket_addr().clone());
            self.dissemination.gossip_join(peer_addr);
        } else {
            // println!("[swim] received duplicate join request for {:?}", peer_addr)
        }
    }

    pub fn handle_ping(&self, sender: UnboundedSender<Response>, peer_addr: NetAddr, gossip_vec: Vec<Gossip>) {
        match self.membership.get(&peer_addr) {
            Some(_) => {
                for gossip in gossip_vec {
                    self.process_gossip(gossip);
                }
                let gossip = self.dissemination.acquire_gossip(&self.membership, 1);
                let message = Response::Ack(self.addr.clone(), gossip);
                let _ = sender.unbounded_send(message).unwrap();
            }
            None => {
                println!("[swim] unknown peer");
            }
        }
    }

    // pub fn handle_ack(&self, peer_addr: NetAddr, gossip_vec: Vec<Gossip>) {
    //     match self.delay {
    //         Some(n) =>
    //             thread::sleep(Duration::from_millis(n)),
    //         None => (),
    //     }

    //     match self.membership.get(&peer_addr) {
    //         Some(entry) => {
    //             for gossip in gossip_vec {
    //                 self.process_gossip(gossip);
    //             }
    //             // Remove timeout if exists
    //             self.timeout_cache.remove_ack_timeout(&peer_addr);
    //             if let Some(forward_addr) = self.timeout_cache.remove_indirect_ack_timeout(&peer_addr) {
    //                 println!("[swim] forwarding ack");
    //                 self.forward_ack(forward_addr.clone(), peer_addr);
    //             }
    //         }
    //         None => {
    //             println!("[swim] unknown peer");
    //         }
    //     }
    // }

    pub fn handle_ping_req(&self, peer_addr: NetAddr, suspect_addr: NetAddr) {
        // if this node is a suspect, immediately ack
        if self.addr == suspect_addr {
            // self.send_ack(peer_addr);
            // gossip alive state
        } else {
            // otherwise initiate a probe
            println!("[swim] initiating probe ({:?},{:?})", peer_addr, suspect_addr);
            self.forward_ping(peer_addr, suspect_addr);
        }
    }

    pub fn handle_query(&self, peer_addr: NetAddr, col: Colour) {
        let slush = self.protocol_state.lock().unwrap();

        slush.insert(peer_addr.clone(), col.clone());
        if slush.col.clone() != Colour::Undecided {
            // self.send_respond(peer_addr, slush.col.clone());
        } else {
            let mut slush = slush;
            slush.set_col(col);
            // self.send_respond(peer_addr, slush.col.clone());
        }
    }

    pub fn handle_respond(&self, peer_addr: NetAddr, col: Colour) {
        let slush = self.protocol_state.lock().unwrap();
        self.timeout_cache.remove_query_timeout(&peer_addr);

        if slush.len() >= K {
            let (red, blue) = slush.outcome();
            let schelling = (0.5 * (K as f32)).round() as u32;
            let mut slush = slush;
            if red > schelling {
                println!("{:?} {}", slush.outcome(), "converged to red".red());
                slush.set_col(Colour::Red);
            }
            if blue > schelling {
                println!("{:?} {}", slush.outcome(), "converged to blue".blue());
                slush.set_col(Colour::Blue);
            }
        }
    }

    pub fn handle_timeouts(&self) {
        if let Ok(Async::Ready(timeouts)) = self.timeout_cache.poll_purge() {
            for expired_addr in timeouts.ack_addr_vec {
                let members = self.membership.sample(1, vec![self.addr.clone()]);
                if members.len() > 0 {
                    let entry = &members[0];
                    let peer_addr = entry.key();
                    // println!("[swim] sampled peer = {:?}", peer_addr.clone());
                    let message = Request::PingReq(self.addr.clone(), expired_addr.clone());
                    self.membership.send(peer_addr.clone(), message);
                    self.timeout_cache.create_ack_timeout(expired_addr);
                }
            }

            for expired_addr in timeouts.indirect_ack_addr_vec {
                // Disseminate suspected
                println!("[swim] proxy request failed, suspect = {:?}", expired_addr.clone());
                self.membership.suspect(expired_addr.clone());
                self.dissemination.gossip_suspect(expired_addr.clone());
                self.timeout_cache.create_suspect_timeout(expired_addr);
            }

            for expired_addr in timeouts.probe_addr_vec {
                println!("[swim] probe failed, suspect = {:?}", expired_addr.clone());
                self.membership.suspect(expired_addr.clone());
                self.dissemination.gossip_suspect(expired_addr.clone());
                self.timeout_cache.create_suspect_timeout(expired_addr);
            }

            for expired_addr in timeouts.suspect_addr_vec {
                // Disseminate confirmed failure
                println!("[swim] failure confirmed = {:?}", expired_addr.clone());
                self.membership.remove(&expired_addr);
                self.dissemination.gossip_confirm(expired_addr);
            }

            for expired_addr in timeouts.query_addr_vec {
                println!("[swim] query to {:?} failed", expired_addr.clone());
            }
        }
    }

    fn process_gossip(&self, gossip: Gossip) {
        println!("[swim] GOSSIP: {:?}", gossip.clone());
        match gossip.clone() {
            Gossip::Join(peer_addr) => {
                if peer_addr.clone() != self.addr {
                    if self.membership.process_join(peer_addr.clone()) {
                        self.send_self_join(peer_addr.to_socket_addr().clone());
                        self.dissemination.gossip_join(peer_addr);
                    }
                }
            }
            // Clear the suspect timeout & mark as alive
            Gossip::Alive(peer_addr) => {
                self.membership.alive(peer_addr.clone());
                self.dissemination.gossip_alive(peer_addr.clone());
                println!("[swim] removing suspect timeout");
                self.timeout_cache.remove_suspect_timeout(&peer_addr);
            }
            // Create a suspect timeout & mark as suspected
            Gossip::Suspect(peer_addr) => {
                self.membership.suspect(peer_addr.clone());
                self.dissemination.gossip_suspect(peer_addr.clone());
                println!("[swim] creating suspect timeout");
                self.timeout_cache.create_suspect_timeout(peer_addr);
            }
            // Remove the peer from the membership map
            Gossip::Confirm(peer_addr) => {
                self.membership.remove(&peer_addr);
                self.dissemination.gossip_confirm(peer_addr);
            }
        }
    }
    
    pub fn run(self) {
        let protocol_period = Duration::from_millis(PROTOCOL_PERIOD);
        let swim = Interval::new(Instant::now(), protocol_period)
            .for_each(move |_instant| {
                println!("[swim] members = {:?}", self.membership.len());
                if self.membership.len() >= 2 {
                    let members = self.membership.sample(1, vec![self.addr.clone()]);
                    if members.len() > 0 {
                        let entry = &members[0];
                        let peer_addr = entry.key();
                        self.send_ping(peer_addr.clone());
                    }

                    if self.membership.len() >= 3 {
                        //----------------------------------------------------------------
                        // Protocol
                        //----------------------------------------------------------------
                        let slush = self.protocol_state.lock().unwrap();
                        if slush.col != Colour::Undecided {
                            // randomly sample from known nodes
                            let members = self.membership.sample(2, vec![self.addr.clone()]);
                            for entry in members {
                                let peer_addr = entry.key();
                                self.send_query(peer_addr.clone(), slush.col.clone())
                            }
                        }
                        //----------------------------------------------------------------
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
