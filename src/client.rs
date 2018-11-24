use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use futures::sync::mpsc;
use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::timer::timeout;
use tokio;
use bincode_channel;
use types::{Request, Response};

pub fn request(peer_addr: SocketAddr, req: Request, timeout: Duration) -> impl Future<Item = Response, Error = timeout::Error<io::Error>> {
    let connect = TcpStream::connect(&peer_addr);
    connect.and_then(|socket| {
        let (read_half, write_half) = socket.split();
        let writer = bincode_channel::new_writer::<Request>(write_half);
        let reader = bincode_channel::new_reader::<Response>(read_half);

        let (tx, rx) = mpsc::unbounded();

        // client request writes request to the channel
        let _ = tx.unbounded_send(req).unwrap();

        let write_request = writer.send_all(
            rx.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "[client] receive error")
            })
        ).then(|_| {
            Ok(())
        });

        tokio::spawn(write_request);

        reader.take(1).collect()
    }).timeout(timeout).and_then(|res| {
        Ok(res[0].clone())
    })
}
