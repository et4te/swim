use std::io;

use futures::{Poll, Sink, StartSend, Stream};
use tokio::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, ReadHalf, WriteHalf};
use bincode_codec::{BincodeReader, BincodeWriter};
use serde::{Serialize, Deserialize};

use message::{Request, Response};

/// A bi-directional bincode channel

pub struct BincodeChannel<T> {
    pub reader: BincodeReader<FramedReader, T>,
    pub writer: BincodeWriter<FramedWriter, T>,
}

impl<T> BincodeChannel<T>
    where
      for<'a> T: Serialize + Deserialize<'a>,
{
    pub fn new(stream: TcpStream) -> BincodeChannel<T> {
        let (reader, writer) = stream.split();
        BincodeChannel {
            reader: new_reader(reader),
            writer: new_writer(writer),
        }
    }
}

impl Sink for BincodeChannel<Request> {
    type SinkItem = Request;
    type SinkError = io::Error;
    
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.writer.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.writer.poll_complete()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.writer.close()
    }

}

impl Stream for BincodeChannel<Response> {
    type Item = Response;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.reader.poll()
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
