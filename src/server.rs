use std::io;
use std::net::SocketAddr;
use std::thread;
use std::sync::{Arc, Mutex};
use futures::sync::mpsc;
use futures::sync::mpsc::UnboundedSender;
use tokio;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use bincode_channel;
use swim::Swim;
use protocol::snowball::Snowball;
use types::{Request, Response};

// TODO Add TLS support

#[derive(Clone)]
pub struct Server {
    pub addr: SocketAddr,
    pub swim: Arc<Swim>,
    pub snowflake: Arc<Mutex<Snowball>>,
}

impl Server {
    pub fn new(addr: SocketAddr, swim: Swim, snowflake: Arc<Mutex<Snowball>>) -> Server {
        Server {
            addr: addr.clone(),
            swim: Arc::new(swim),
            snowflake: snowflake,
        }
    }

    pub fn bootstrap(&self, bootstrap_addr: SocketAddr) {
        self.swim.send_bootstrap_join(bootstrap_addr);
    }

    fn process_input(self, sender: UnboundedSender<Response>, request: Request) {
        debug!("RECV={:?}", request.clone());
        match request.clone() {
            // SWIM
            Request::Join(peer_addr) =>
                self.swim.handle_join(sender, peer_addr),
            Request::Ping(_peer_addr, gossip_vec) =>
                self.swim.handle_ping(sender, gossip_vec),
            Request::PingReq(_peer_addr, suspect_addr) =>
                self.swim.handle_ping_req(sender, suspect_addr),
            // Protocol
            Request::Query(_peer_addr, col) => {
                let mut snowflake = self.snowflake.lock().unwrap();
                snowflake.handle_query(sender, col);
            }
        }
    }

    fn handle_connection(self, socket: TcpStream) {
        // Splits the socket stream into bincode reader / writers
        let (read_half, write_half) = socket.split();
        let writer = bincode_channel::new_writer::<Response>(write_half);
        let reader = bincode_channel::new_reader::<Request>(read_half);

        // Creates sender and receiver channels in order to read and
        // write data from / to the socket
        let (tx, rx) = mpsc::unbounded();

        // Process the incoming messages from the socket reader stream
        let input_reader = reader
            .for_each(move |message| {
                let () = self.clone().process_input(tx.clone(), message);
                Ok(())
            }).map_err(|err| {
                error!("handle_connection => {:?}", err);
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
                    error!("spawn => {:?}", err);
                });
            info!("listening at {:?}", bind_addr);
            tokio::run(server)
        })
    }

}

