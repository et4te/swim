use std::time::Duration;
use std::collections::HashMap;
use rand::{self, Rng};
use tokio::prelude::*;
use tokio;
use colored::Colorize;
use ::types::{NetAddr, Response, Request, ResponseTx};
use protocol::types::{Colour, ColourTx, ColourRx};
use membership::Membership;
use client;
use constants::ROUND_TRIP_TIME;

const A: f32 = 0.5;
const B: u32 = 11;
const K: usize = 4;

pub struct Snowball {
    pub addr: NetAddr,
    pub col: Colour,
    pub lastcol: Colour,
    pub cnt: u32,
    pub d: HashMap<Colour, u32>,
}

impl Snowball {

    pub fn new(addr: NetAddr) -> Snowball {
        let mut rng = rand::thread_rng();
        let r: usize = rng.gen_range(0, 3);
        let col = match r {
            0 => {
                debug!("started undecided");
                Colour::Undecided
            },
            1 => {
                debug!("{}", "started red".red());
                Colour::Red
            },
            2 => {
                debug!("{}", "started blue".blue());
                Colour::Blue
            },
            _ => {
                error!("not possible");
                Colour::Undecided
            },
        };
        let mut d = HashMap::new();
        let _ = d.insert(Colour::Red, 0u32);
        let _ = d.insert(Colour::Blue, 0u32);
        Snowball { addr, col: col.clone(), lastcol: col, cnt: 0u32, d }
    }

    pub fn set_col(&mut self, col: Colour) {
        self.col = col;
    }

    pub fn set_lastcol(&mut self, col: Colour) {
        self.lastcol = col;
    }

    pub fn set_cnt(&mut self, cnt: u32) {
        self.cnt = cnt;
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
                warn!("send_query => {:?}", err);
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

    pub fn run(&mut self, tx: &ColourTx, rx: &mut ColourRx, membership: &Membership) -> bool {
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
                    self.d.entry(Colour::Red).and_modify(|d| *d += 1);
                    if self.d[&Colour::Red] > *self.d.get(&self.col).unwrap() {
                        debug!("{:?} {}", (red, blue), "converged to red".red());
                        self.set_col(Colour::Red);
                    }
                    
                    if self.lastcol == Colour::Red {
                        let cnt = self.cnt.clone();
                        self.set_cnt(cnt + 1);
                        if self.cnt > B {
                            debug!("{}", "decided on red".red());
                            return true;
                        }
                    } else {
                        debug!("{:?} {}", (red, blue), "lastcol set to red".red());
                        self.set_lastcol(Colour::Red);
                        self.set_cnt(0u32);
                    }
                }

                if blue > quiescent_point {
                    self.d.entry(Colour::Blue).and_modify(|d| *d += 1);
                    if self.d[&Colour::Blue] > *self.d.get(&self.col).unwrap() {
                        debug!("{:?} {}", (red, blue), "converged to blue".blue());
                        self.set_col(Colour::Blue);
                    }
                    
                    if self.lastcol == Colour::Blue {
                        let cnt = self.cnt.clone();
                        self.set_cnt(cnt + 1);
                        if self.cnt > B {
                            debug!("{}", "decided on blue".blue());
                            return true;
                        }
                    } else {
                        debug!("{:?} {}", (red, blue), "lastcol set to blue".blue());
                        self.set_lastcol(Colour::Blue);
                        self.set_cnt(0u32);
                    }
                }
            }
        }

        return false;
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
                error!("undecided outcome"),
        }
    }
    (red, blue)
}
