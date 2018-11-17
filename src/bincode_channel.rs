use std::io;

use futures::{Poll, Sink, StartSend, Stream};
use tokio::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, ReadHalf, WriteHalf};
use bincode_codec::{BincodeReader, BincodeWriter};
use serde::{Serialize, Deserialize};

use message::Message;

/// A bi-directional bincode channel

pub struct BincodeChannel<T> {
    pub r: BincodeReader<FramedReader, T>,
    pub w: BincodeWriter<FramedWriter, T>,
}

impl<T> BincodeChannel<T>
    where
      for<'a> T: Serialize + Deserialize<'a>,
{
    pub fn new(stream: TcpStream) -> BincodeChannel<T> {
        let (reader, writer) = stream.split();
        BincodeChannel {
            r: new_reader(reader),
            w: new_writer(writer),
        }
    }
}

impl Sink for BincodeChannel<Message> {
    type SinkItem = Message;
    type SinkError = io::Error;
    
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.w.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.w.poll_complete()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.w.close()
    }

}

impl Stream for BincodeChannel<Message> {
    type Item = Message;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.r.poll()
    }
}

/// Utility

pub type FramedReader = FramedRead<ReadHalf<TcpStream>, LengthDelimitedCodec>;
pub type FramedWriter = FramedWrite<WriteHalf<TcpStream>, LengthDelimitedCodec>;

pub fn new_reader<T>(read_half: ReadHalf<TcpStream>) -> BincodeReader<FramedReader, T>
    where
      for<'a> T: Deserialize<'a>
{
    let length_delimited = FramedRead::new(read_half, LengthDelimitedCodec::new());
    BincodeReader::<_, T>::new(length_delimited)
}

pub fn new_writer<T>(write_half: WriteHalf<TcpStream>) -> BincodeWriter<FramedWriter, T>
    where
      T: Serialize
{
    let length_delimited = FramedWrite::new(write_half, LengthDelimitedCodec::new());
    BincodeWriter::new(length_delimited)
}
