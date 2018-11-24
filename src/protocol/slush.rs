use std::time::Duration;
use rand::{self, Rng};
use tokio::prelude::*;
use tokio;
use colored::Colorize;
use ::types::{NetAddr, Response, Request, ResponseTx};
use protocol::types::{Colour, ColourTx, ColourRx};
use membership::Membership;
use client;
use constants::ROUND_TRIP_TIME;

// The protocol alpha parameter
const A: f32 = 0.5;
// The number of peers probed
const K: usize = 4;

#[derive(Clone)]
pub struct Slush {
    pub addr: NetAddr,
    pub col: Colour,
}

impl Slush {

    pub fn new(addr: NetAddr) -> Slush {
        let mut rng = rand::thread_rng();
        let r: usize = rng.gen_range(0, 3);
        let col = match r {
            0 => {
                println!("[slush] started undecided");
                Colour::Undecided
            },
            1 => {
                println!("{}", "[slush] started red".red());
                Colour::Red
            },
            2 => {
                println!("{}", "[slush] started blue".blue());
                Colour::Blue
            },
            _ => panic!("[slush] not possible"),
        };
        Slush { addr, col }
    }

    pub fn set_col(&mut self, col: Colour) {
        self.col = col;
    }

    pub fn send_query(&self, tx: ColourTx, peer_addr: NetAddr) {
        let timeout = Duration::from_millis(ROUND_TRIP_TIME);
        let message = Request::Query(self.addr.clone(), self.col.clone());
        let send = client::request(peer_addr.to_socket_addr().clone(), message, timeout)
            .and_then(move |message| {
                if let Response::Respond(col) = message {
                    let _ = tx.unbounded_send(col).unwrap();
                }
                Ok(())
            }).map_err(move |err| {
                println!("[client] query error = {:?}", err);
                ()
            });
        tokio::spawn(send);
    }

    pub fn handle_query(&mut self, tx: ResponseTx, col: Colour) {
        if self.col.clone() != Colour::Undecided {
            tx.unbounded_send(Response::Respond(self.col.clone())).unwrap();
        } else {
            self.set_col(col);
            tx.unbounded_send(Response::Respond(self.col.clone())).unwrap();
        }
    }

    pub fn run(&mut self, tx: &ColourTx, rx: &mut ColourRx, membership: &Membership) {
        if membership.len() >= K {
            if self.col != Colour::Undecided {
                
                // randomised round robin sampling from known nodes
                let members = membership.sample(K, vec![self.addr.clone()]);
                for peer_addr in members {
                    self.send_query(tx.clone(), peer_addr.clone());
                }

                let mut v: Vec<Colour> = vec![];
                let mut i = 0;
                while let Ok(Async::Ready(Some(col))) = rx.poll() {
                    v.push(col);
                    i += 1;
                    if i >= K {
                        break;
                    }
                }

                let (red, blue) = outcome(v);
                let quiescent_point = (A * (K as f32)).round() as u32;

                if red > quiescent_point {
                    println!("{:?} {}", (red, blue), "converged to red".red());
                    self.set_col(Colour::Red);
                }

                if blue > quiescent_point {
                    println!("{:?} {}", (red, blue), "converged to blue".blue());
                    self.set_col(Colour::Blue);
                }
            }
        }
    }
}

pub fn outcome(votes: Vec<Colour>) -> (u32, u32) {
    let mut red: u32 = 0;
    let mut blue: u32 = 0;
    for col in votes.iter() {
        match col {
            Colour::Red =>
                red += 1,
            Colour::Blue =>
                blue += 1,
            Colour::Undecided =>
                panic!("[slush] undecided outcome"),
        }
    }
    (red, blue)
}

