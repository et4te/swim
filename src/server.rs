use std::io;
use std::net::SocketAddr;
use std::thread;
use std::sync::Arc;
use futures::sync::mpsc;
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use uuid::Uuid;
use bincode_channel::BincodeChannel;
use cache::TimeoutCache;
use membership::Membership;
use dissemination::Dissemination;
use message::{Message, Gossip};
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

    fn process_input(self, message: Message) {
        // println!("[server] RECV: {:?}", message.clone());
        match message.clone() {
            Message::Join(peer_uuid, peer_addr) =>
                self.swim.handle_join(peer_uuid, peer_addr),
            Message::Ping(peer_uuid, gossip_vec) =>
                self.swim.handle_ping(peer_uuid, gossip_vec),
            Message::Ack(peer_uuid, gossip_vec) =>
                self.swim.handle_ack(peer_uuid, gossip_vec),
            Message::PingReq(peer_uuid, suspect_uuid) =>
                self.swim.handle_ping_req(peer_uuid, suspect_uuid),
        }
    }

    fn handle_connection(self, socket: TcpStream) {
        // Splits the socket stream into bincode reader / writers
        let message_channel = BincodeChannel::<Message>::new(socket);

        // Creates sender and receiver channels in order to read and
        // write data from / to the socket
        let (_sender, receiver) = mpsc::unbounded();

        // Process the incoming messages from the socket reader stream
        let input_reader = message_channel
            .reader
            .for_each(move |message| {
                let () = self.clone().process_input(message);
                Ok(())
            }).map_err(|err| {
                println!("[server] Error processing input = {:?}", err);
            });

        tokio::spawn(input_reader);

        // Send all messages received in the receiver stream to the
        // socket writer sink
        let output_writer = message_channel
            .writer
            .send_all(
                receiver
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

