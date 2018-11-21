use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use rand::{self, Rng};
use message::NetAddr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Colour {
    Undecided,
    Red,
    Blue,
}

pub struct Slush {
    pub col: Colour,
    pub votes: SkipMap<NetAddr, Colour>,
}

impl Slush {

    pub fn new() -> Slush {
        let mut rng = rand::thread_rng();
        let r: usize = rng.gen_range(0, 3);
        let col = match r {
            0 => Colour::Undecided,
            1 => Colour::Red,
            2 => Colour::Blue,
            _ => panic!("error colour"),
        };
        Slush { col, votes: SkipMap::new() }
    }

    pub fn len(&self) -> usize {
        self.votes.len()
    }

    pub fn set_col(&mut self, col: Colour) {
        self.col = col;
    }

    pub fn insert(&self, peer_addr: NetAddr, col: Colour) {
        self.votes.insert(peer_addr, col);
    }

    pub fn outcome(&self) -> (u32, u32) {
        let mut red: u32 = 0;
        let mut blue: u32 = 0;
        for entry in self.votes.iter() {
            let col = entry.value();
            match col {
                Colour::Red =>
                    red += 1,
                Colour::Blue =>
                    blue += 1,
                _ =>
                    panic!("undecided was stored in votes"),
            }
        }
        (red, blue)
    }
}


