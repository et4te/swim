extern crate bincode;
extern crate bytes;
extern crate clap;

#[macro_use]
extern crate futures;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate tokio;
extern crate tokio_serde;

mod bincode_codec;
mod bincode_channel;
mod message;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use std::net::SocketAddr;
use std::io;
use futures::sync::mpsc;
use tokio::prelude::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::timer::{self, delay_queue, Interval, DelayQueue};
use rand::Rng;
use clap::{Arg, App, SubCommand};
use message::{Gossip, Message};
use bincode_channel::BincodeChannel;

enum PeerState {
    Alive,
    Suspected,
    Confirmed,
}

struct Cache {
    // Permanent peers
    membership_map: HashMap<SocketAddr, PeerState>,
    // Ack expirations
    expiration_map: HashMap<SocketAddr, delay_queue::Key>,
    // Forwarding requests from other peers
    forwarding_map: HashMap<SocketAddr, (delay_queue::Key, SocketAddr)>,
    // The amount of updates per membership update disseminated
    updates_map: HashMap<Gossip, u32>,
    ack_expirations: DelayQueue<SocketAddr>,
    indirect_ack_expirations: DelayQueue<SocketAddr>,
    suspected_expirations: DelayQueue<SocketAddr>,
}

const PROTOCOL_PERIOD: u64 = 1000;
const ROUND_TRIP_TIME: u64 = 100;

// If an Ack is received at exactly the same time as a cache entry has
// expired then a race condition occurs.

impl Cache {

    fn new() -> Cache {
        Cache {
            membership_map: HashMap::new(),
            expiration_map: HashMap::new(),
            forwarding_map: HashMap::new(),
            updates_map: HashMap::new(),
            ack_expirations: DelayQueue::new(),
            indirect_ack_expirations: DelayQueue::new(),
            suspected_expirations: DelayQueue::new(),
        }
    }

    fn sample_peer(&self, exclude: Option<SocketAddr>) -> Option<SocketAddr> {
        match exclude {
            Some(addr) => {
                let keys: Vec<&SocketAddr> = self.membership_map.keys()
                    .filter(|key| **key != addr)
                    .collect();
                if keys.len() > 0 {
                    let mut rng = rand::thread_rng();
                    let i: usize = rng.gen_range(0, keys.len());
                    Some(keys[i].clone())
                } else {
                    None
                }
            }
            None => {
                let keys: Vec<&SocketAddr> = self.membership_map.keys().collect();
                if keys.len() > 0 {
                    let mut rng = rand::thread_rng();
                    let i: usize = rng.gen_range(0, keys.len());
                    Some(keys[i].clone())
                } else {
                    None
                }
            }
        }
    }

    // When a Ping message is sent to a peer, a record is stored in a
    // DelayQueue which expires within round_trip_time. 
    fn record_ping(&mut self, peer_addr: SocketAddr) {
        println!("[cache] Inserting ack_expiration peer_addr = {:?}", peer_addr.clone());
        let delay = self.ack_expirations
            .insert(peer_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
        self.expiration_map.insert(peer_addr, delay);
        self.membership_map.insert(peer_addr, PeerState::Alive);
    }

    // If the Ping is made due to an indirect Ping request from another
    // peer, the forwarding map is used to store the address of the
    // peer to forward a succkessful Ack to.
    fn record_ping_request(&mut self, peer_addr: SocketAddr, suspect_addr: SocketAddr) {
        println!("[cache] Inserting indirect_ack_expiration suspect_addr = {:?}", suspect_addr.clone());
        let delay = self.indirect_ack_expirations
            .insert(suspect_addr.clone(), Duration::from_millis(ROUND_TRIP_TIME));
        self.forwarding_map.insert(suspect_addr, (delay, peer_addr));
    }

    // When an Ack message is received from a peer, if an associated
    // expiration exists then it is removed. 
    fn remove_expiration(&mut self, peer_addr: &SocketAddr) {
        if let Some(expiration_key) = self.expiration_map.remove(peer_addr) {
            println!("[cache] Removing ack_expiration peer_addr = {:?}", peer_addr.clone());
            self.ack_expirations.remove(&expiration_key);
        }

        if let Some((expiration_key, forward_addr)) = self.forwarding_map.remove(peer_addr) {
            println!("[cache] Removing indirect_ack_expiration");
            self.indirect_ack_expirations.remove(&expiration_key);
            let suspect_addr = peer_addr.clone();
            let forward = client_send(forward_addr, Message::Ack(suspect_addr, vec![]));
            tokio::spawn(forward);
        }
    }

    // Mark a suspect (downgrade state) due to a failure to Ack in time.
    fn mark(&mut self, suspect_addr: SocketAddr) -> Gossip {
        unimplemented!()
    }

    // Queue dissemination information such that it is piggy-backed onto later
    // messages.
    fn disseminate(&mut self, gossip: Gossip) {
        unimplemented!()
    }

    // When an Ack is not received in time, an indirect probe is initiated and
    // a second ack_expiration is recorded. If the second ack_expiration expires
    // the suspect is marked and the information is disseminated.
    fn poll_purge(&mut self, self_addr: SocketAddr) -> Poll<(), timer::Error> {
        while let Some(expired) = try_ready!(self.ack_expirations.poll()) {
            let suspect_addr = expired.get_ref().clone();
            println!("[swim] ack expired for {:?}", suspect_addr.clone());
            let _ = self.expiration_map.remove(&suspect_addr).unwrap();
            if let Some(peer_addr) = self.sample_peer(Some(suspect_addr.clone())) {
                println!("[swim] Probing {:?}", suspect_addr.clone());
                let probe = client_send(peer_addr, Message::PingReq(self_addr, suspect_addr));
                tokio::spawn(probe);
            } else {
                println!("[swim] No peer was found besides the current suspect ...");
            }
        }

        // The indirect ack requests are also checked. If these have expired then
        // they are simply removed from the list.
        while let Some(expired) = try_ready!(self.indirect_ack_expirations.poll()) {
            println!("[swim] indirect ack expiration");
            let suspect_addr = expired.get_ref().clone();
            let _ = self.forwarding_map.remove(&suspect_addr).unwrap();
        }

        Ok(Async::Ready(()))
    }
}

fn client_send(peer_addr: SocketAddr, message: Message) -> impl Future<Item = (), Error = ()> {
    let connect = TcpStream::connect(&peer_addr.clone());
    connect.and_then(move |socket| {
        println!("[client] Connected to peer at {:?}", peer_addr.clone());
        let message_channel = BincodeChannel::<Message>::new(socket);
        let (sender, receiver) = mpsc::unbounded();

        // Since this is a client send start by sending the message
        println!("[client] SEND: {:?}", message.clone());
        let _ = sender.unbounded_send(message).unwrap();

        // Send everything in receiver to sink
        let output_writer = message_channel.w.send_all(
            receiver.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "[client] Receiver error")
            })
        ).then(|_| {
            Ok(())
        });
        
        tokio::spawn(output_writer);

        Ok(())
    }).map_err(|e| {
        // When the connection to a peer fails, ...
        println!("[client:error] Accept error = {:?}", e);
    })
}

fn process(self_addr: SocketAddr, cache: Arc<Mutex<Cache>>, message: Message, delay: Option<u64>) {
    match message.clone() {
        Message::Ping(peer_addr, _v) => {
            // Send Ack
            match delay {
                Some(d) =>
                    std::thread::sleep(Duration::from_millis(d)),
                None => ()
            }
            println!("[server] RECV: {:?}", message.clone());
            let ack = client_send(peer_addr, Message::Ack(self_addr, vec![]));
            tokio::spawn(ack);

            // Queue disseminated information
        },
        Message::Ack(peer_addr, _v) => {
            // Remove expiration
            println!("[server] RECV: {:?}", message.clone());
            cache.lock().unwrap().remove_expiration(&peer_addr);

            // Queue disseminated information
        },
        Message::PingReq(peer_addr, suspect_addr) => {
            println!("[server] RECV: {:?}", message.clone());

            // If this node is suspected, immediately ack
            if self_addr == suspect_addr {
                let ack = client_send(peer_addr, Message::Ack(self_addr, vec![]));
                tokio::spawn(ack);
            } else {
                // Otherwise initiate a probe
                let probe = client_send(suspect_addr.clone(), Message::Ping(self_addr.clone(), vec![]));
                cache.lock().unwrap()
                    .record_ping_request(peer_addr, suspect_addr);
                tokio::spawn(probe);
            }
        }
    }
}

fn main() {
    let matches = App::new("SWIM")
        .version("1.0")
        .arg(Arg::with_name("address")
             .short("a")
             .long("address")
             .value_name("IP:PORT")
             .help("The address of this node")
             .required(true)
             .takes_value(true))
        .arg(Arg::with_name("bootstrap")
             .short("b")
             .long("bootstrap")
             .value_name("IP:PORT")
             .help("The address of a seed to bootstrap to")
             .takes_value(true))
        .arg(Arg::with_name("delay")
             .short("d")
             .long("delay")
             .value_name("MS")
             .help("An artificial delay introduced for testing")
             .takes_value(true))
        .get_matches();

    let bind_addr: SocketAddr = matches.value_of("address").unwrap()
        .parse().unwrap();

    let delay: Option<u64> = match matches.value_of("delay") {
        Some(s) => Some(s.parse().unwrap()),
        None => None,
    };

    let self_addr = bind_addr.clone();
    let cache = Arc::new(Mutex::new(Cache::new()));
    let cache_server = cache.clone();
    let cache_client = cache.clone();

    std::thread::spawn(move || {
        let bind_addr = bind_addr.clone();
        let listener = TcpListener::bind(&bind_addr.clone()).unwrap();
        let server = listener.incoming().for_each(move |stream| {
            let message_channel = BincodeChannel::<Message>::new(stream);
            let (sender, receiver) = mpsc::unbounded();

            // Process the incoming stream
            let cache_server = cache_server.clone();
            let delay = delay.clone();
            let input_reader = message_channel.r.for_each(move |message| {
                process(bind_addr.clone(), cache_server.clone(), message.clone(), delay.clone());
                Ok(())
            }).map_err(|e| {
                println!("[server] Input reader error = {:?}", e);
            });

            tokio::spawn(input_reader);

            // Send everything in receiver to sink
            let output_writer = message_channel.w.send_all(
                receiver.map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "[server] Receiver error")
                })
            ).then(|_| Ok(()));

            tokio::spawn(output_writer);

            Ok(())
        }).map_err(|e| {
            println!("[server:error] Accept error = {:?}", e);
        });

        println!("[server] Listening on {:?}", bind_addr);
        tokio::run(server);
    });

    if let Some(bootstrap_addr) = matches.value_of("bootstrap") {
        let bootstrap_addr: SocketAddr = bootstrap_addr.parse().unwrap();
        let bootstrap = client_send(bootstrap_addr.clone(), Message::Ping(bind_addr.clone(), vec![]));
        cache_client.lock().unwrap()
            .record_ping(bootstrap_addr);
        // println!(">> Running bootstrap ...");
        tokio::run(bootstrap);
        // println!(">> Bootstrap complete.");
    }

    let interval = Duration::from_millis(PROTOCOL_PERIOD);
    let swim = Interval::new(Instant::now(), interval)
        .for_each(move |instant| {
            if let Some(peer_addr) = cache.lock().unwrap().sample_peer(None) {
                println!("[swim] Sample {:?}", peer_addr.clone());
                let probe = client_send(peer_addr, Message::Ping(self_addr.clone(), vec![]));
                tokio::spawn(probe);
            }
            let _ = cache.lock().unwrap().poll_purge(self_addr.clone());
            Ok(())
        }).map_err(|e| {
            panic!("Interval error; err={:}", e);
        });
    
    tokio::run(swim);
}
