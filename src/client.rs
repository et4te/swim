use std::io;
use std::net::SocketAddr;
use futures::sync::mpsc;
use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio;
use message::Message;
use bincode_channel::BincodeChannel;

pub fn send(peer_addr: SocketAddr, message: Message) -> impl Future<Item = (), Error = ()> {
    let connect = TcpStream::connect(&peer_addr.clone());
    connect.and_then(move |socket| {
        // println!("[client] Connected to peer at {:?}", peer_addr.clone());
        let message_channel = BincodeChannel::<Message>::new(socket);
        let (sender, receiver) = mpsc::unbounded();

        // Since this is a client send start by sending the message
        // println!("[client] SEND: {:?}", message.clone());
        let _ = sender.unbounded_send(message).unwrap();

        // Send everything in receiver to sink
        let output_writer = message_channel.writer.send_all(
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

pub fn spawn_send(peer_addr: SocketAddr, message: Message) {
    let send = send(peer_addr, message);
    tokio::spawn(send);
}

pub fn run_send(peer_addr: SocketAddr, message: Message) {
    let send = send(peer_addr, message);
    tokio::run(send);
}
