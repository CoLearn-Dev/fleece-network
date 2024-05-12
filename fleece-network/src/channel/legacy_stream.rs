use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, Sink, SinkExt, Stream, StreamExt};
use libp2p::PeerId;
use serde::{de::DeserializeOwned, Serialize};
use sync_wrapper::SyncWrapper;

use crate::utils::buflist::BufList;

enum ReadPhase {
    Header([u8; 4], usize),
    Payload(BytesMut, usize),
}

pub struct BinStream<T> {
    stream: SyncWrapper<Pin<Box<dyn AsyncRead + Send>>>,
    phase: ReadPhase,
    _t: PhantomData<T>,
}

impl<T> BinStream<T> {
    pub fn new(stream: Pin<Box<dyn AsyncRead + Send>>) -> Self {
        Self {
            stream: SyncWrapper::new(stream),
            phase: ReadPhase::Header([0; 4], 0),
            _t: PhantomData,
        }
    }

    fn split_borrow(
        &mut self,
    ) -> (
        &mut SyncWrapper<Pin<Box<dyn AsyncRead + Send>>>,
        &mut ReadPhase,
    ) {
        (&mut self.stream, &mut self.phase)
    }
}

impl<T: DeserializeOwned + Unpin> Stream for BinStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let (stream, phase) = self.split_borrow();
            let stream = Pin::new(stream).get_pin_mut();
            match phase {
                ReadPhase::Header(ref mut buffer, mut saved) => {
                    match stream.poll_read(cx, &mut buffer[saved..]) {
                        Poll::Ready(result) => match result {
                            Ok(len) => {
                                saved += len;
                                if saved == 4 {
                                    let len = u32::from_be_bytes(*buffer) as usize;
                                    self.phase =
                                        ReadPhase::Payload(BytesMut::with_capacity(len), 0);
                                }
                            }
                            Err(_) => return Poll::Ready(None),
                        },
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ReadPhase::Payload(ref mut buffer, saved) => {
                    match stream.poll_read(cx, &mut buffer[*saved..]) {
                        Poll::Ready(result) => match result {
                            Ok(len) => {
                                let saved = *saved + len;
                                if saved == buffer.len() {
                                    let message: T = bincode::deserialize(&buffer).unwrap();
                                    self.phase = ReadPhase::Header([0; 4], 0);
                                    return Poll::Ready(Some(message));
                                }
                            }
                            Err(_) => return Poll::Ready(None),
                        },
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

pub struct BinSink<T> {
    stream: SyncWrapper<Pin<Box<dyn AsyncWrite + Send>>>,
    buffer: BufList<Bytes>,
    _t: PhantomData<T>,
}

impl<T> BinSink<T> {
    pub fn new(stream: Pin<Box<dyn AsyncWrite + Send>>) -> Self {
        Self {
            stream: SyncWrapper::new(stream),
            buffer: BufList::default(),
            _t: PhantomData,
        }
    }

    fn split_borrow(
        &mut self,
    ) -> (
        &mut SyncWrapper<Pin<Box<dyn AsyncWrite + Send>>>,
        &mut BufList<Bytes>,
    ) {
        (&mut self.stream, &mut self.buffer)
    }
}

impl<T: Serialize + Unpin> Sink<T> for BinSink<T> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&item)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        self.buffer.push(Bytes::from(bytes));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            let (stream, buffer) = self.split_borrow();
            let stream = Pin::new(stream).get_pin_mut();
            if !buffer.has_remaining() {
                return match stream.poll_flush(cx) {
                    Poll::Ready(result) => match result {
                        Ok(()) => Poll::Ready(Ok(())),
                        Err(e) => {
                            Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                        }
                    },
                    Poll::Pending => Poll::Pending,
                };
            }
            match stream.poll_write(cx, buffer.chunk()) {
                Poll::Ready(result) => match result {
                    Ok(len) => {
                        buffer.advance(len);
                    }
                    Err(e) => {
                        return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                    }
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => (),
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }

        let stream = Pin::new(&mut self.stream).get_pin_mut();
        stream.poll_close(cx)
    }
}

pub struct PeerStream<T> {
    pub peer_id: PeerId,
    stream: BinStream<T>,
}

impl<T> PeerStream<T> {
    pub fn new(peer_id: PeerId, stream: Pin<Box<dyn AsyncRead + Send>>) -> Self {
        Self {
            peer_id,
            stream: BinStream::new(stream),
        }
    }
}

impl<T: DeserializeOwned + Unpin> Stream for PeerStream<T> {
    type Item = (PeerId, T);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(result) => Poll::Ready(result.map(|item| (self.peer_id.clone(), item))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct PeerSink<T> {
    pub peer_id: PeerId,
    sink: BinSink<T>,
}

impl<T> PeerSink<T> {
    pub fn new(peer_id: PeerId, stream: Pin<Box<dyn AsyncWrite + Send>>) -> Self {
        Self {
            peer_id,
            sink: BinSink::new(stream),
        }
    }
}

impl<T: Serialize + Unpin> Sink<T> for PeerSink<T> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.sink.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_close_unpin(cx)
    }
}
