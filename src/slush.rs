use std::time::Duration;
use rand::{self, Rng};
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::prelude::*;
use tokio;
use colored::Colorize;
use message::{NetAddr, Response, Request};
use membership::Membership;
use client;
use constants::ROUND_TRIP_TIME;

const K: usize = 2;

type ColourTx = UnboundedSender<Colour>;
type ColourRx = UnboundedReceiver<Colour>;
type ResponseTx = UnboundedSender<Response>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Colour {
    Undecided,
    Red,
    Blue,
}

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
            0 => Colour::Undecided,
            1 => Colour::Red,
            2 => Colour::Blue,
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
        if membership.len() > 3 {
            if self.col != Colour::Undecided {
                let mut v = vec![];

                // randomly sample from known nodes
                let members = membership.sample(3, vec![self.addr.clone()]);
                for entry in members {
                    let peer_addr = entry.key();
                    self.send_query(tx.clone(), peer_addr.clone());
                }

                // try receive as many as possible
                while let Ok(Async::Ready(x)) = rx.poll() {
                    v.push(x)
                }

                let (red, blue) = outcome(v);
                let quiescent = (0.5 * (K as f32)).round() as u32;

                if red > quiescent {
                    println!("{:?} {}", (red, blue), "converged to red".red());
                    self.set_col(Colour::Red);
                }

                if blue > quiescent {
                    println!("{:?} {}", (red, blue), "converged to blue".blue());
                    self.set_col(Colour::Blue);
                }
            }
        }
    }
}

pub fn outcome(votes: Vec<Option<Colour>>) -> (u32, u32) {
    let mut red: u32 = 0;
    let mut blue: u32 = 0;
    for col in votes.iter() {
        match col {
            Some(Colour::Red) =>
                red += 1,
            Some(Colour::Blue) =>
                blue += 1,
            Some(Colour::Undecided) =>
                panic!("[slush] undecided outcome"),
            None =>
                panic!("[slush] no outcome"),
        }
    }
    (red, blue)
}

