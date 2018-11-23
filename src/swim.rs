use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use futures::sync::mpsc::{self, UnboundedSender};
use tokio::prelude::*;
use tokio::timer::Interval;
use tokio;
use membership::Membership;
use dissemination::Dissemination;
use client;
use cache::TimeoutCache;
use message::{NetAddr, Request, Response, Gossip};
use constants::{PROTOCOL_PERIOD, ROUND_TRIP_TIME};
use slush::Slush;

#[derive(Clone)]
pub struct Swim {
    pub addr: NetAddr,
    delay: Option<u64>,
    membership: Arc<Membership>,
    dissemination: Arc<Dissemination>,
    timeout_cache: Arc<TimeoutCache>,
}

impl Swim {

    pub fn new(addr: SocketAddr, delay: Option<u64>) -> Swim {
        Swim {
            addr: NetAddr::new(addr),
            delay,
            membership: Arc::new(Membership::new()),
            dissemination: Arc::new(Dissemination::new()),
            timeout_cache: Arc::new(TimeoutCache::new()),
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
                println!("[swim] bootstrap error = {:?}", err)
            });
        tokio::run(request);
    }

    pub fn request_self_join(&self, peer_addr: SocketAddr) {
        let timeout = Duration::from_millis(ROUND_TRIP_TIME);
        let message = Request::Join(self.addr.clone());
        let request = client::request(peer_addr, message, timeout)
            .and_then(|_message| {
                Ok(())
            })
            .map_err(|err| {
                println!("[swim] self_join error = {:?}", err)
            });
        tokio::spawn(request);
    }

    pub fn send_self_join(&self, sender: UnboundedSender<Response>) {
        let message = Response::Join(self.addr.clone());
        let _ = sender.unbounded_send(message).unwrap();
    }

    pub fn send_ping(self, peer_addr: NetAddr) {
        let self_1 = self.clone();
        let self_2 = self.clone();
        let timeout = Duration::from_millis(ROUND_TRIP_TIME);
        let gossip = self.dissemination.acquire_gossip(&self.membership);
        let message = Request::Ping(self.addr.clone(), gossip);
        let request = client::request(peer_addr.to_socket_addr().clone(), message, timeout)
            .and_then(move |message| {
                if let Response::Ack(gossip_vec) = message {
                    for gossip in gossip_vec {
                        self_1.process_gossip(gossip);
                    }
                } // else error
                Ok(())
            })
            .map_err(move |err| {
                // if a timeout occurs, initiate a probe
                println!("[client] ping error = {:?}", err);
                self_2.send_ping_req(peer_addr);
            });
        tokio::spawn(request);
    }

    pub fn send_ping_req(self, suspect_addr: NetAddr) {
        let members = self.membership.sample(1, vec![self.addr.clone()]);
        if members.len() > 0 {
            println!("[client] initiating probe");
            let self_1 = self.clone();
            let self_2 = self.clone();
            let timeout = Duration::from_millis(ROUND_TRIP_TIME);            
            let message = Request::PingReq(self.addr.clone(), suspect_addr.clone());
            let request = client::request(suspect_addr.to_socket_addr().clone(), message, timeout)
                .and_then(move |message| {
                    // suspect replies indirectly via probe
                    if let Response::Ack(gossip_vec) = message {
                        for gossip in gossip_vec {
                            self_1.process_gossip(gossip);
                        }
                    }
                    Ok(())
                })
                .map_err(move |err| {
                    // probe timeout, suspect
                    println!("[client] ping_req error = {:?}", err);
                    self_2.timeout_cache.create_suspect_timeout(suspect_addr.clone());
                });
            tokio::spawn(request);
        }
    }

    pub fn handle_join(&self, sender: UnboundedSender<Response>, peer_addr: NetAddr) {
        if self.membership.process_join(peer_addr.clone()) {
            self.send_self_join(sender);
            self.dissemination.gossip_join(peer_addr);
        } else {
            println!("[swim] received duplicate join request for {:?}", peer_addr);
        }
    }

    pub fn handle_ping(&self, sender: UnboundedSender<Response>, gossip_vec: Vec<Gossip>) {
        for gossip in gossip_vec {
            self.process_gossip(gossip);
        }
        let gossip = self.dissemination.acquire_gossip(&self.membership);
        let message = Response::Ack(gossip);
        let _ = sender.unbounded_send(message).unwrap();
    }

    pub fn handle_ping_req(&self, sender: UnboundedSender<Response>, suspect_addr: NetAddr) {
        // if this node is a suspect, immediately ack
        if self.addr == suspect_addr {
            // gossip alive state?
            let gossip = self.dissemination.acquire_gossip(&self.membership);
            let message = Response::Ack(gossip);
            let _ = sender.unbounded_send(message);
        } else {
            println!("[swim] initiating probe into {:?}", suspect_addr);
            // send ping to suspect
            let self_1 = self.clone();
            let self_2 = self.clone();
            let timeout = Duration::from_millis(ROUND_TRIP_TIME);
            let gossip = self.dissemination.acquire_gossip(&self.membership);
            let message = Request::Ping(self.addr.clone(), gossip);
            let request = client::request(suspect_addr.to_socket_addr().clone(), message, timeout)
                .and_then(move |message| {
                    // if suspect Acks within RTT -> send ack to sender
                    if let Response::Ack(gossip_vec) = message.clone() {
                        for gossip in gossip_vec {
                            self_1.process_gossip(gossip);
                        }
                        let _ = sender.unbounded_send(message).unwrap();
                    } // else error
                    Ok(())
                })
                .map_err(move |err| {
                    // else suspect ...
                    println!("[swim] ping_req error = {:?}", err);
                    self_2.timeout_cache.create_suspect_timeout(suspect_addr);
                });
            tokio::spawn(request);
        }
    }

    pub fn handle_timeouts(&self) {
        if let Ok(Async::Ready(timeouts)) = self.timeout_cache.poll_purge() {
            for expired_addr in timeouts.suspect_addr_vec {
                // Disseminate confirmed failure
                println!("[swim] failure confirmed = {:?}", expired_addr.clone());
                self.membership.remove(&expired_addr);
                self.dissemination.gossip_confirm(expired_addr);
            }
        }
    }

    fn process_gossip(&self, gossip: Gossip) {
        println!("[swim] GOSSIP: {:?}", gossip.clone());
        match gossip.clone() {
            Gossip::Join(peer_addr) => {
                if peer_addr.clone() != self.addr {
                    if self.membership.process_join(peer_addr.clone()) {
                        self.request_self_join(peer_addr.to_socket_addr().clone());
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
    
    pub fn run(self, slush: Arc<Mutex<Slush>>) {
        let (tx, mut rx) = mpsc::unbounded();
        let protocol_period = Duration::from_millis(PROTOCOL_PERIOD);
        let swim = Interval::new(Instant::now(), protocol_period)
            .for_each(move |_instant| {
                println!("[swim] members = {:?}", self.membership.len());
                if self.membership.len() >= 2 {
                    let addrs = self.membership.sample_rr(1, vec![self.addr.clone()]);
                    if addrs.len() > 0 {
                        let peer_addr = addrs[0].clone();
                        self.clone().send_ping(peer_addr.clone());
                    }
                    
                    slush.lock().unwrap()
                        .run(&tx, &mut rx, &self.membership);

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
