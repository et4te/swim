use futures::sync::mpsc;
use std::io;
use std::net::SocketAddr;
use std::thread;
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use uuid::Uuid;
use bincode_channel::BincodeChannel;
use cache::TimeoutCache;
use client::{run_send, spawn_send};
use membership::Membership;
use dissemination::Dissemination;
use message::{Message, Gossip};

#[derive(Clone)]
pub struct Server {
    pub uuid: Uuid,
    pub addr: SocketAddr,
}

impl Server {
    pub fn new(addr: SocketAddr) -> Server {
        Server {
            addr,
            uuid: Uuid::new_v4(),
        }
    }

    pub fn send_bootstrap_join(&self, seed_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        run_send(seed_addr, message);
    }

    fn send_join(&self, peer_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        spawn_send(peer_addr, message);
    }

    fn send_ack(&self, peer_addr: SocketAddr, gossip: Vec<Gossip>) {
        let message = Message::Ack(self.uuid.clone(), gossip);
        spawn_send(peer_addr, message);
    }

    fn send_ping(&self, peer_addr: SocketAddr) {
        let message = Message::Ping(self.uuid.clone(), vec![]);
        spawn_send(peer_addr, message);
    }

    fn process_gossip<'a>(&'a self, gossip: Gossip, membership: &'a Membership, dissemination: &'a Dissemination) {
        match gossip.clone() {
            Gossip::Join(peer_uuid, peer_addr_string) => {
                if peer_uuid != self.uuid {
                    println!("[server] GOSSIP: {:?}", gossip);
                    let peer_addr = peer_addr_string.parse().unwrap();
                    if membership.process_join(peer_uuid, peer_addr) {
                        self.send_join(peer_addr);
                        dissemination.gossip_join(peer_uuid, peer_addr);
                    }
                }
            }
            _ => ()
        }
    }

    fn process_input(
        self,
        message: Message,
        mut membership: Membership,
        mut dissemination: Dissemination,
        cache: TimeoutCache,
    ) {
        println!("[server] RECV: {:?}", message.clone());
        match message.clone() {
            Message::Join(peer_uuid, peer_addr) => {
                if membership.process_join(peer_uuid, peer_addr) {
                    self.send_join(peer_addr);
                    dissemination.gossip_join(peer_uuid, peer_addr);
                } else {
                    println!("[server] Failed to process join request for {:?}", peer_addr);
                }
            }
            Message::Ping(peer_uuid, gossip_vec) => {
                match membership.get(peer_uuid) {
                    Some(entry) => {
                        // Process dissemination information
                        for gossip in gossip_vec {
                            self.process_gossip(gossip, &membership, &dissemination);
                        }
                        // Attach dissemination information
                        let (peer_addr, _) = entry.value();
                        let gossip = dissemination.acquire_gossip(membership.clone(), 1);
                        self.send_ack(*peer_addr, gossip);
                    }
                    None => {
                        println!("[server] unknown peer");
                    }
                }
            }
            Message::Ack(peer_uuid, gossip_vec) => {
                match membership.get(peer_uuid) {
                    Some(_) => {
                        // Process dissemination information
                        for gossip in gossip_vec {
                            self.process_gossip(gossip, &membership, &dissemination);
                        }
                        // Remove timeout if exists
                        println!("[server] Removing timeout for {:?}", peer_uuid.clone());
                        cache.remove_ack_timeout(peer_uuid);
                    }
                    None => {
                        println!("[server] unknown peer");
                    }
                }
            }
            Message::PingReq(peer_uuid, suspect_uuid) => {
                match membership.get(peer_uuid) {
                    Some(entry) => {
                        let (peer_addr, _) = entry.value();
                        match membership.get(suspect_uuid) {
                            Some(entry) => {
                                let (suspect_addr, _) = entry.value();
                                // If this node is a suspect, immediately ack
                                if self.addr == *suspect_addr {
                                    let gossip = dissemination.acquire_gossip(membership.clone(), 1);
                                    self.send_ack(*peer_addr, gossip);
                                } else {
                                    // Otherwise initiate a probe
                                    self.send_ping(*suspect_addr);
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
        }
    }

    fn handle_connection(self, socket: TcpStream, membership: Membership, dissemination: Dissemination, cache: TimeoutCache) {
        // Splits the socket stream into bincode reader / writers
        let message_channel = BincodeChannel::<Message>::new(socket);

        // Creates sender and receiver channels in order to read and
        // write data from / to the socket
        let (_sender, receiver) = mpsc::unbounded();

        // Process the incoming messages from the socket reader stream
        let input_reader = message_channel
            .reader
            .for_each(move |message| {
                let () = self
                    .clone()
                    .process_input(message, membership.clone(), dissemination.clone(), cache.clone());
                Ok(())
            }).map_err(|err| {
                println!("[server] Error processing input = {:?}", err);
            });

        tokio::spawn(input_reader);

        // Send all messages received in the receiver stream to the
        // socket writer sink
        let output_writer = message_channel
            .writer
            .send_all(
                receiver
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "[server] Receiver error")),
            ).then(|_| Ok(()));

        tokio::spawn(output_writer);
    }

    pub fn spawn(self, membership: Membership, dissemination: Dissemination, cache: TimeoutCache) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let bind_addr = self.addr.clone();
            let listener = TcpListener::bind(&bind_addr).unwrap();
            let server = listener
                .incoming()
                .for_each(move |socket| {
                    let () =
                        self.clone()
                            .handle_connection(socket, membership.clone(), dissemination.clone(), cache.clone());
                    Ok(())
                }).map_err(|err| {
                    println!("[server] Accept error = {:?}", err);
                });
            println!("[server] Listening at {:?}", bind_addr);
            tokio::run(server)
        })
    }
}
