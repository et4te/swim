use std::io;
use std::thread;
use std::net::SocketAddr;
use futures::sync::mpsc;
use tokio::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tokio;
use uuid::Uuid;
use bincode_channel::BincodeChannel;
use client::{run_send, spawn_send};
use membership::Membership;
use cache::TimeoutCache;
use message::Message;

#[derive(Clone)]
pub struct Server {
    pub uuid: Uuid,
    pub addr: SocketAddr,
}

impl Server {

    pub fn new(addr: SocketAddr) -> Server {
        Server { addr, uuid: Uuid::new_v4() }
    }

    pub fn send_bootstrap_join(&self, seed_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        run_send(seed_addr, message);
    }

    fn send_join(&self, peer_addr: SocketAddr) {
        let message = Message::Join(self.uuid.clone(), self.addr.clone());
        spawn_send(peer_addr, message);
    }

    fn send_ack(&self, peer_addr: SocketAddr) {
        let message = Message::Ack(self.uuid.clone(), vec![]);
        spawn_send(peer_addr, message);
    }

    fn send_ping(&self, peer_addr: SocketAddr) {
        let message = Message::Ping(self.uuid.clone(), vec![]);
        spawn_send(peer_addr, message);
    }

    fn process_input(self, message: Message, mut membership: Membership, cache: TimeoutCache) {
        println!("[server] RECV: {:?}", message.clone());
        match message.clone() {
            Message::Join(peer_uuid, peer_addr) => {
                if membership.process_join(peer_uuid, peer_addr) {
                    self.send_join(peer_addr);
                }

                // Queue dissemination message
                // self.dissemination_queue.unbounded_send(Gossip::Join(peer_addr));
            }
            Message::Ping(peer_uuid, _v) => {
                match membership.get(peer_uuid) {
                    Some(entry) => {
                        // Attach dissemination information
                        let (peer_addr, _) = entry.value();
                        self.send_ack(*peer_addr);
                    }
                    None => {
                        println!("[server] unknown peer");
                    }
                }
            }
            Message::Ack(peer_uuid, _v) => {
                match membership.get(peer_uuid) {
                    Some(_) => {
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
                                    self.send_ack(*peer_addr);
                                } else { // Otherwise initiate a probe
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

    fn handle_connection(self, socket: TcpStream, membership: Membership, cache: TimeoutCache) {
        // Splits the socket stream into bincode reader / writers
        let message_channel = BincodeChannel::<Message>::new(socket);

        // Creates sender and receiver channels in order to read and
        // write data from / to the socket
        let (_sender, receiver) = mpsc::unbounded();

        // Process the incoming messages from the socket reader stream
        let input_reader = message_channel.r.for_each(move |message| {
            let () = self.clone().process_input(message, membership.clone(), cache.clone());
            Ok(())
        }).map_err(|err| {
            println!("[server] Error processing input = {:?}", err);
        });

        tokio::spawn(input_reader);

        // Send all messages received in the receiver stream to the
        // socket writer sink
        let output_writer = message_channel.w.send_all(
            receiver.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "[server] Receiver error")
            })
        ).then(|_| Ok(()));

        tokio::spawn(output_writer);
    }

    pub fn spawn(self, membership: Membership, cache: TimeoutCache) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let bind_addr = self.addr.clone();
            let listener = TcpListener::bind(&bind_addr).unwrap();
            let server = listener.incoming().for_each(move |socket| {
                let () = self.clone().handle_connection(socket, membership.clone(), cache.clone());
                Ok(())
            }).map_err(|err| {
                println!("[server] Accept error = {:?}", err);
            });
            println!("[server] Listening at {:?}", bind_addr);
            tokio::run(server)
        })
    }
}
