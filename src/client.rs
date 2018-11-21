use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use futures::sync::mpsc;
use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::timer::timeout;
use tokio;
use message::{Request, Response};
use bincode_channel::{self, BincodeChannel};

pub fn request(peer_addr: SocketAddr, req: Request, timeout: Duration) -> impl Future<Item = Response, Error = timeout::Error<io::Error>> {
    let connect = TcpStream::connect(&peer_addr);
    connect.and_then(|socket| {
        let (read_half, write_half) = socket.split();
        let writer = bincode_channel::new_writer::<Request>(write_half);
        let mut reader = bincode_channel::new_reader::<Response>(read_half);

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

pub fn send(peer_addr: SocketAddr, message: Request) -> impl Future<Item = (), Error = ()> {
    let connect = TcpStream::connect(&peer_addr.clone());
    connect.and_then(move |socket| {
        // println!("[client] Connected to peer at {:?}", peer_addr.clone());
        let (read_half, write_half) = socket.split();
        let writer = bincode_channel::new_writer::<Request>(write_half);
        // let mut reader = bincode_channel::new_reader::<Response>(read_half);

        let (sender, receiver) = mpsc::unbounded();

        // Since this is a client send start by sending the message
        // println!("[client] SEND: {:?}", message.clone());
        let _ = sender.unbounded_send(message).unwrap();

        // Send everything in receiver to sink
        let output_writer = writer.send_all(
            receiver.map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "[client] Receiver error")
            })
        ).then(|_| {
            Ok(())
        });
        
        tokio::spawn(output_writer);

        Ok(())
    }).map_err(|e| {
        // Suspect this peer
        println!("[client] connect error = {:?}", e);
    })
}

pub fn spawn_send(peer_addr: SocketAddr, message: Request) {
    let send = send(peer_addr, message);
    tokio::spawn(send);
}

pub fn run_send(peer_addr: SocketAddr, message: Request) {
    let send = send(peer_addr, message);
    tokio::run(send);
}

pub fn run_request(peer_addr: SocketAddr, message: Request, timeout: Duration) {
    let request = request(peer_addr, message, timeout)
        .and_then(|response| {
            println!("[client] response = {:?}", response);
            Ok(())
        })
        .map_err(|err| {
            println!("[client] timeout {:?}", err);
        });
    tokio::run(request)
}
