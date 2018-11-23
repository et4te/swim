extern crate bincode;
extern crate bytes;
extern crate colored;
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

mod bincode_codec;
mod bincode_channel;
mod constants;
mod message;
mod membership;
mod dissemination;
mod server;
mod client;
mod cache;
mod swim;
mod slush;

use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use clap::{Arg, App};
use server::Server;
use swim::Swim;
use slush::Slush;

fn main() {
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
    let slush = Arc::new(Mutex::new(Slush::new(swim.addr.clone())));
    let server = Server::new(bind_addr.clone(), swim.clone(), slush.clone());
    server.clone().spawn();

    if let Some(bootstrap_addr) = matches.value_of("bootstrap") {
        let bootstrap_addr: SocketAddr = bootstrap_addr.parse().unwrap();
        server.bootstrap(bootstrap_addr);
    }

    swim.run(slush);
}
