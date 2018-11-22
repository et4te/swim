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
}

impl Slush {

    pub fn new() -> Slush {
        let mut rng = rand::thread_rng();
        let r: usize = rng.gen_range(0, 3);
        let col = match r {
            0 => Colour::Undecided,
            1 => Colour::Red,
            2 => Colour::Blue,
            _ => panic!("[slush] error colour"),
        };
        Slush { col }
    }

    pub fn set_col(&mut self, col: Colour) {
        self.col = col;
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


