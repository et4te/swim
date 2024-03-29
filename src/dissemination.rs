use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use membership::Membership;
use constants::GOSSIP_RATE;
use types::{NetAddr, Gossip};

type GossipMap = SkipMap<Gossip, usize>;

#[derive(Clone)]
pub struct Dissemination {
    gossip_map: Arc<GossipMap>,
}

impl Dissemination {

    pub fn new() -> Dissemination {
        Dissemination {
            gossip_map: Arc::new(SkipMap::new()),
        }
    }

    pub fn try_gossip(&self, gossip: Gossip) {
        match self.gossip_map.get(&gossip) {
            Some(_) => (),
            None => {
                self.gossip_map.insert(gossip.clone(), 0);
            }
        }
    }

    pub fn gossip_join(&self, peer_addr: NetAddr) {
        let gossip = Gossip::Join(peer_addr);
        self.try_gossip(gossip)
    }

    pub fn gossip_alive(&self, peer_addr: NetAddr) {
        let gossip = Gossip::Alive(peer_addr);
        self.try_gossip(gossip)
    }

    pub fn gossip_suspect(&self, peer_addr: NetAddr) {
        let gossip = Gossip::Suspect(peer_addr);
        self.try_gossip(gossip)
    }

    pub fn gossip_confirm(&self, peer_addr: NetAddr) {
        let gossip = Gossip::Confirm(peer_addr);
        self.try_gossip(gossip)
    }

    pub fn acquire_gossip<'a>(&'a self, membership: &'a Membership) -> Vec<Gossip> {
        let member_count = membership.len();
        let gossip_rate = GOSSIP_RATE * (((member_count + 1) as f64).ln().ceil() as usize);
        debug!("gossip_rate = {:?}", gossip_rate);
        let mut gossip_vec = vec![];
        if self.gossip_map.len() > 0 {
            for entry in self.gossip_map.iter() {
                let gossip = entry.key();
                // Join messages are disseminated continuously until MINIMUM_MEMBERS has been
                // reached.
                let dissemination_count = entry.value();
                if let Gossip::Join(_) = gossip {
                    if *dissemination_count > gossip_rate {
                        ()
                    } else {
                        debug!("dissemination_count({:?}), gossip_rate ({:?})",
                              *dissemination_count, gossip_rate);
                        self.gossip_map.insert(gossip.clone(), dissemination_count + 1);
                        gossip_vec.push(gossip.clone())
                    }
                } else {
                    if *dissemination_count > gossip_rate {
                        ()
                    } else {
                        debug!("dissemination_count({:?}), gossip_rate ({:?})",
                              *dissemination_count, gossip_rate);
                        self.gossip_map.insert(gossip.clone(), dissemination_count + 1);
                        gossip_vec.push(gossip.clone())
                    }
                }
            }
        }
        return gossip_vec;
    }
}
