use std::io;
use std::net::SocketAddr;
use std::thread;
use std::sync::Arc;
use futures::sync::mpsc;
use futures::sync::mpsc::UnboundedSender;
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use bincode_channel;
use message::{Request, Response};
use swim::Swim;

#[derive(Clone)]
pub struct Server {
    pub addr: SocketAddr,
    pub swim: Arc<Swim>,
}

impl Server {
    pub fn new(addr: SocketAddr, swim: Swim) -> Server {
        Server {
            addr: addr.clone(),
            swim: Arc::new(swim),
        }
    }

    pub fn bootstrap(&self, bootstrap_addr: SocketAddr) {
        self.swim.send_bootstrap_join(bootstrap_addr);
    }

    fn process_input(self, sender: UnboundedSender<Response>, request: Request) {
        println!("[server] RECV: {:?}", request.clone());
        match request.clone() {
            // SWIM
            Request::Join(peer_addr) =>
                self.swim.handle_join(sender, peer_addr),
            Request::Ping(peer_addr, gossip_vec) =>
                self.swim.handle_ping(sender, peer_addr, gossip_vec),
            Request::PingReq(peer_addr, suspect_addr) =>
                self.swim.handle_ping_req(sender, suspect_addr),
            // Protocol
            Request::Query(peer_addr, col) =>
                self.swim.handle_query(sender, col),
        }
    }

    fn handle_connection(self, socket: TcpStream) {
        // Splits the socket stream into bincode reader / writers
        let (read_half, write_half) = socket.split();
        let writer = bincode_channel::new_writer::<Response>(write_half);
        let mut reader = bincode_channel::new_reader::<Request>(read_half);

        // Creates sender and receiver channels in order to read and
        // write data from / to the socket
        let (tx, rx) = mpsc::unbounded();

        // Process the incoming messages from the socket reader stream
        let input_reader = reader
            .for_each(move |message| {
                let () = self.clone().process_input(tx.clone(), message);
                Ok(())
            }).map_err(|err| {
                println!("[server] Error processing input = {:?}", err);
            });

        tokio::spawn(input_reader);

        // Send all messages received in the receiver stream to the
        // socket writer sink
        let output_writer = writer
            .send_all(
                rx
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, "[server] Receiver error")),
            ).then(|_| Ok(()));

        tokio::spawn(output_writer);
    }

    pub fn spawn(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let bind_addr = self.addr.clone();
            let listener = TcpListener::bind(&bind_addr).unwrap();
            let server = listener
                .incoming()
                .for_each(move |socket| {
                    let () = self.clone().handle_connection(socket);
                    Ok(())
                }).map_err(|err| {
                    println!("[server] Accept error = {:?}", err);
                });
            println!("[server] Listening at {:?}", bind_addr);
            tokio::run(server)
        })
    }

}

