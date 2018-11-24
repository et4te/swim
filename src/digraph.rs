use std::fmt;

struct Digraph {
    v: u32,             // number of vertices
    e: u32,             // number of edges
    adj: Vec<Vec<u32>>, // adj[v] = adjacency list for vertex v
    indegree: Vec<u32>, // indegree[v] = indegree of vertex v
}

impl fmt::Debug for Digraph {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?} vertices, {:?} edges\n", self.v(), self.e());
        for v in 0..(self.v() as usize) {
            write!(f, "{:?}: ", v);
            for w in self.adj[v as usize].clone() {
                write!(f, "{:?} ", w);
            }
            write!(f, "\n");
        }
        Ok(())
    }
}

impl Digraph {

    pub fn new(v: u32) -> Digraph {
        assert!(v > 0);
        let adj = vec![vec![]; v as usize];
        let indegree = vec![0u32; v as usize];
        Digraph { v, e: 0, adj, indegree }
    }

    pub fn v(&self) -> u32 {
        self.v
    }

    pub fn e(&self) -> u32 {
        self.e
    }

    fn validate_vertex(&self, v: u32) {
        if v < 0 || v >= self.v {
            panic!("vertex is not between 0 and v - 1")
        }
    }

    pub fn add_edge(&mut self, v: u32, w: u32) {
        self.validate_vertex(v);
        self.validate_vertex(w);
        self.adj[v as usize].push(w);
        self.indegree[w as usize] += 1;
        self.e += 1;
    }

    pub fn adj(&self, v: u32) -> Vec<u32> {
        self.validate_vertex(v);
        self.adj[v as usize].clone()
    }

    pub fn outdegree(&self, v: u32) -> usize {
        self.validate_vertex(v);
        self.adj[v as usize].len()
    }

    pub fn indegree(&self, v: u32) -> u32 {
        self.validate_vertex(v);
        self.indegree[v as usize]
    }

    pub fn reverse(&self) -> Digraph {
        let mut reverse = Digraph::new(self.v);
        for v in 0..self.v {
            for w in self.adj(v) {
                reverse.add_edge(w, v);
            }
        }
        reverse
    }

    // pub fn stack_tc(&self) {
    //     let cstack = vec![];
    //     let vstack = vec![];

    //     let root = v;
    //     let comp = None;
    //     vstack.push(v);
    //     let saved_height = cstack.len();

    //     let mut self_loop = false;

    //     for w in self.adj[v] {
    //         if w == v {
    //             self_loop = true;
    //         } else {
                
    //         }
    //     }
        
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one() {
        let mut genesis = Digraph::new(1);
        genesis.add_edge(0, 2);
        println!("{:?}", genesis);
    }
}
