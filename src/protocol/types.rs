use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver};

#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub enum Colour {
    Undecided,
    Red,
    Blue,
}

pub type ColourTx = UnboundedSender<Colour>;
pub type ColourRx = UnboundedReceiver<Colour>;
