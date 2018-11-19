use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use crossbeam_skiplist::SkipMap;
use crossbeam::deque::{self, Worker, Stealer, Steal};
use uuid::Uuid;
use membership::Membership;
use message::Gossip;

const MINIMUM_MEMBERS: usize = 3;
const GOSSIP_RATE: usize = 2;

type GossipMap = SkipMap<Gossip, usize>;

#[derive(Clone)]
pub struct Dissemination {
    gossip_map: Arc<GossipMap>,
    gossip_worker: Arc<Mutex<Worker<Gossip>>>,
    gossip_stealer: Arc<Stealer<Gossip>>,
}

impl Dissemination {

    pub fn new() -> Dissemination {
        let (worker, stealer) = deque::lifo::<Gossip>();
        Dissemination {
            gossip_map: Arc::new(SkipMap::new()),
            gossip_worker: Arc::new(Mutex::new(worker)),
            gossip_stealer: Arc::new(stealer),
        }
    }

    pub fn gossip_join(&self, peer_uuid: Uuid, peer_addr: SocketAddr) {
        let gossip = Gossip::Join(peer_uuid, peer_addr.to_string());
        match self.gossip_map.get(&gossip) {
            Some(entry) => (),
            None => {
                self.gossip_map.insert(gossip.clone(), 0);
                self.gossip_worker.lock().unwrap()
                    .push(gossip);
            }
        }
    }

    pub fn acquire_gossip(&self, membership: Membership, limit: usize) -> Vec<Gossip> {
        let member_count = membership.len();
        println!("[dissemination] log {:?} = {:?}",
                 member_count.clone(),
                 (member_count.clone() as f64).ln(),
        );
        let gossip_rate = GOSSIP_RATE * ((member_count as f64).ln().trunc() as usize);
        println!("[dissemination] gossip_rate = {:?}", gossip_rate);
        let mut gossip_vec = vec![];
        while gossip_vec.len() < limit {
            if let Steal::Data(gossip) = self.gossip_stealer.steal() {
                match self.gossip_map.get(&gossip) {
                    Some(entry) => {
                        // Join messages are disseminated continuously until MINIMUM_MEMBERS
                        // has been reached.
                        let dissemination_count = entry.value();
                        if let Gossip::Join(_, _) = gossip {
                            if (member_count > MINIMUM_MEMBERS) && (*dissemination_count > gossip_rate) {
                                self.gossip_map.remove(entry.key());
                            } else {
                                self.gossip_worker.lock().unwrap()
                                    .push(gossip.clone());
                                self.gossip_map.insert(gossip.clone(), dissemination_count + 1);
                                gossip_vec.push(gossip);
                            }
                        } else {
                            if *dissemination_count > gossip_rate {
                                self.gossip_map.remove(entry.key());
                            } else {
                                self.gossip_worker.lock().unwrap()
                                    .push(gossip.clone());
                                self.gossip_map.insert(gossip.clone(), dissemination_count + 1);
                                gossip_vec.push(gossip);
                            }
                        }
                    }
                    None => {
                        println!("[dissemination] Error gossip not present in map");
                        return gossip_vec;
                    }
                }
            } else {
                return gossip_vec;
            }
        }
        return gossip_vec;
    }
}