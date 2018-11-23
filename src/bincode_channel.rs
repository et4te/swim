use tokio::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio::net::TcpStream;
use tokio::io::{ReadHalf, WriteHalf};
use bincode_codec::{BincodeReader, BincodeWriter};
use serde::{Serialize, Deserialize};

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
