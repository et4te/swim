extern crate bincode;
extern crate bytes;
extern crate colored;
extern crate clap;
extern crate crossbeam;
extern crate crossbeam_skiplist;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate tokio;
extern crate tokio_serde;

mod bincode_codec;
mod bincode_channel;
mod cache;
mod client;
mod constants;
mod digraph;
mod dissemination;
mod membership;
mod protocol;
mod server;
mod swim;
mod types;

use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use clap::{Arg, App};
use server::Server;
use swim::Swim;
use protocol::snowball::Snowball;

fn main() {
    pretty_env_logger::init();

    let matches = App::new("swim")
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

    let swim = Swim::new(bind_addr.clone(), delay);
    let snowball = Arc::new(Mutex::new(Snowball::new(swim.addr.clone())));
    let server = Server::new(bind_addr.clone(), swim.clone(), snowball.clone());
    server.clone().spawn();

    if let Some(bootstrap_addr) = matches.value_of("bootstrap") {
        let bootstrap_addr: SocketAddr = bootstrap_addr.parse().unwrap();
        server.bootstrap(bootstrap_addr);
    }

    swim.run(snowball);
}
