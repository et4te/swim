extern crate bincode;
extern crate bytes;
extern crate clap;
extern crate crossbeam_skiplist;
#[macro_use]
extern crate futures;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate tokio;
extern crate tokio_serde;
extern crate uuid;

mod bincode_codec;
mod bincode_channel;
mod message;
mod membership;
mod server;
mod client;
mod cache;

use std::time::{Instant, Duration};
use std::net::SocketAddr;
use tokio::prelude::*;
use tokio::timer::Interval;
use clap::{Arg, App};
use server::Server;
use membership::Membership;
use message::Message;
use cache::TimeoutCache;

enum PeerState {
    Alive,
    Suspected,
    Confirmed,
}

const PROTOCOL_PERIOD: u64 = 1000;
const ROUND_TRIP_TIME: u64 = 1;

fn maybe_delay(delay: Option<u64>) {
    match delay {
        Some(ms) =>
            std::thread::sleep(Duration::from_millis(ms)),
        None =>
            ()
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

    // let delay: Option<u64> = match matches.value_of("delay") {
    //     Some(s) => Some(s.parse().unwrap()),
    //     None => None,
    // };

    let membership = Membership::new();
    let cache = TimeoutCache::new();
    let server = Server::new(bind_addr.clone());
    server.clone().spawn(membership.clone(), cache.clone());

    if let Some(bootstrap_addr) = matches.value_of("bootstrap") {
        let bootstrap_addr: SocketAddr = bootstrap_addr.parse().unwrap();
        server.send_bootstrap_join(bootstrap_addr);
    }

    let interval = Duration::from_millis(PROTOCOL_PERIOD);
    let swim = Interval::new(Instant::now(), interval)
        .for_each(move |_instant| {
            println!("members_count = {:?}", membership.len());
            if membership.len() >= 2 {
                let members = membership.sample(1, vec![server.uuid.clone()]);
                if members.len() > 0 {
                    let entry = &members[0];
                    let peer_uuid = entry.key();
                    println!("[swim] Sampled peer = {:?}", peer_uuid.clone());
                    let message = Message::Ping(server.uuid.clone(), vec![]);
                    membership.send(peer_uuid.clone(), message);
                    cache.create_ack_timeout(*peer_uuid);
                }

                if let Ok(Async::Ready((ack_uuids, indirect_ack_uuids))) = cache.poll_purge() {
                    for expired_uuid in ack_uuids {
                        let members = membership.sample(1, vec![server.uuid.clone()]);
                        if members.len() > 0 {
                            let entry = &members[0];
                            let peer_uuid = entry.key();
                            println!("[swim] Sampled peer = {:?}", peer_uuid.clone());
                            let message = Message::PingReq(server.uuid.clone(), expired_uuid);
                            membership.send(peer_uuid.clone(), message);
                            cache.create_indirect_ack_timeout(*peer_uuid);
                        }
                    }

                    for expired_uuid in indirect_ack_uuids {
                        // Disseminate suspected
                        println!("[disseminate] Disseminating suspect = {:?}", expired_uuid);
                    }
                }

                Ok(())
            } else {
                Ok(())
            }
        }).map_err(|e| {
            panic!("Interval error; err={:}", e);
        });
    
    tokio::run(swim);
}
