use std::marker::PhantomData;
use std::io;
use bincode;
use bytes::{Bytes, BytesMut};
use futures::{Poll, Sink, StartSend, Stream};
use serde::{Deserialize, Serialize};
use tokio_serde::{Deserializer, FramedRead, FramedWrite, Serializer};

/// Bincode

struct Bincode<T> {
    ghost: PhantomData<T>,
}

pub struct BincodeReader<T, U> {
    inner: FramedRead<T, U, Bincode<U>>,
}

pub struct BincodeWriter<T: Sink, U> {
    inner: FramedWrite<T, U, Bincode<U>>,
}

impl<T, U> BincodeReader<T, U>
where
    T: Stream<Error = io::Error>,
    for<'a> U: Deserialize<'a>,
    BytesMut: From<T::Item>,
{
    pub fn new(inner: T) -> BincodeReader<T, U> {
        let bincode = Bincode { ghost: PhantomData };
        BincodeReader {
            inner: FramedRead::new(inner, bincode),
        }
    }
}

impl<T, U> BincodeReader<T, U> {
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T, U> Stream for BincodeReader<T, U>
where
    T: Stream<Error = io::Error>,
    for<'a> U: Deserialize<'a>,
    BytesMut: From<T::Item>,
{
    type Item = U;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<U>, io::Error> {
        self.inner.poll()
    }
}

impl<T, U> Sink for BincodeReader<T, U>
where
    T: Sink,
{
    type SinkItem = T::SinkItem;
    type SinkError = T::SinkError;

    fn start_send(&mut self, item: T::SinkItem) -> StartSend<T::SinkItem, T::SinkError> {
        self.get_mut().start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), T::SinkError> {
        self.get_mut().poll_complete()
    }

    fn close(&mut self) -> Poll<(), T::SinkError> {
        self.get_mut().close()
    }
}

impl<T, U> BincodeWriter<T, U>
where
    T: Sink<SinkItem = Bytes, SinkError = io::Error>,
    U: Serialize,
{
    pub fn new(inner: T) -> BincodeWriter<T, U> {
        let bincode = Bincode { ghost: PhantomData };
        BincodeWriter {
            inner: FramedWrite::new(inner, bincode),
        }
    }
}

impl<T: Sink, U> BincodeWriter<T, U> {
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T, U> Sink for BincodeWriter<T, U>
where
    T: Sink<SinkItem = Bytes, SinkError = io::Error>,
    U: Serialize,
{
    type SinkItem = U;
    type SinkError = io::Error;

    fn start_send(&mut self, item: U) -> StartSend<U, io::Error> {
        self.inner.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.inner.poll_complete()
    }

    fn close(&mut self) -> Poll<(), io::Error> {
        self.inner.close()
    }
}

impl<T, U> Stream for BincodeWriter<T, U>
where
    T: Stream + Sink,
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<T::Item>, T::Error> {
        self.get_mut().poll()
    }
}

impl<T> Deserializer<T> for Bincode<T>
where
    for<'a> T: Deserialize<'a>,
{
    type Error = io::Error;

    fn deserialize(&mut self, src: &BytesMut) -> Result<T, io::Error> {
        bincode::deserialize(src)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

impl<T: Serialize> Serializer<T> for Bincode<T> {
    type Error = io::Error;

    fn serialize(&mut self, item: &T) -> Result<Bytes, io::Error> {
        bincode::serialize(item)
            .map(Into::into)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
