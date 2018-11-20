extern crate bincode;
extern crate bytes;
extern crate clap;
extern crate crossbeam;
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
mod dissemination;
mod server;
mod client;
mod cache;
mod swim;

use std::time::{Instant, Duration};
use std::net::SocketAddr;
use tokio::prelude::*;
use tokio::timer::Interval;
use clap::{Arg, App};
use server::Server;
use membership::Membership;
use dissemination::Dissemination;
use message::Message;
use cache::TimeoutCache;
use swim::Swim;

enum PeerState {
    Alive,
    Suspected,
    Confirmed,
}

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

    let swim = Swim::new(bind_addr.clone());
    let server = Server::new(bind_addr.clone(), swim.clone());
    server.clone().spawn();

    if let Some(bootstrap_addr) = matches.value_of("bootstrap") {
        let bootstrap_addr: SocketAddr = bootstrap_addr.parse().unwrap();
        server.bootstrap(bootstrap_addr);

        // server.swim.send_bootstrap_join(bootstrap_addr);
        // dissemination.gossip_join(server.uuid.clone(), server.addr.clone());
    }

    swim.run();
}
