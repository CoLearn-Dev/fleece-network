use std::{
    fmt::Debug,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use sync_wrapper::SyncWrapper;

use super::message::{InboundMessage, OutboundMessage};

pub trait Codec: Clone {
    /// The type of protocol(s) or protocol versions being negotiated.
    type Protocol: AsRef<str> + Send + Clone + Debug;
    /// The type of inbound and outbound requests.
    type Request: DeserializeOwned + Serialize + Send + Unpin + Debug;
    /// The type of inbound and outbound responses.
    type Response: DeserializeOwned + Serialize + Send + Unpin + Debug;

    type Encoder: Encoder<Message = OutboundMessage<Self::Request, Self::Response>> + Send + Unpin;

    type Decoder: Decoder<Message = InboundMessage<Self::Request, Self::Response>> + Send + Unpin;

    fn new_encoder(&self) -> Self::Encoder;

    fn new_decoder(&self) -> Self::Decoder;
}

pub trait Encoder {
    type Message: DeserializeOwned + Serialize + Send + Unpin + Debug;

    fn start_send(&mut self, payload: &Self::Message) -> Result<(), io::Error>;

    fn poll_flush(
        &mut self,
        writer: Pin<&mut (impl AsyncWrite + Unpin + Send)>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>;
}

pub trait Decoder {
    type Message: DeserializeOwned + Serialize + Send + Unpin + Debug;

    /// Reads a request from the given I/O stream according to the
    /// negotiated protocol.
    fn poll_read(
        &mut self,
        reader: Pin<&mut (impl AsyncRead + Unpin + Send)>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<Self::Message>>;
}

pub struct CodecStream<D: Decoder> {
    stream: SyncWrapper<Pin<Box<dyn AsyncRead + Send + Unpin>>>,
    decoder: D,
}

impl<D: Decoder> CodecStream<D> {
    pub fn new(stream: impl AsyncRead + Send + Unpin + 'static, decoder: D) -> Self {
        Self {
            stream: SyncWrapper::new(Box::pin(stream) as Pin<Box<dyn AsyncRead + Send + Unpin>>),
            decoder,
        }
    }

    pub fn split_borrow(
        &mut self,
    ) -> (
        &mut SyncWrapper<Pin<Box<dyn AsyncRead + Send + Unpin>>>,
        &mut D,
    ) {
        (&mut self.stream, &mut self.decoder)
    }
}

impl<D: Decoder + Unpin> Stream for CodecStream<D> {
    type Item = D::Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (stream, codec) = self.split_borrow();
        let stream = Pin::new(stream).get_pin_mut();

        // as long as there is an error, the whole stream should be marked as failed
        match codec.poll_read(stream, cx) {
            Poll::Ready(result) => Poll::Ready(result.ok()),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct CodecSink<E: Encoder> {
    sink: SyncWrapper<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
    encoder: E,
}

impl<E: Encoder> CodecSink<E> {
    pub fn new(stream: impl AsyncWrite + Send + Unpin + 'static, encoder: E) -> Self {
        Self {
            sink: SyncWrapper::new(Box::pin(stream) as Pin<Box<dyn AsyncWrite + Send + Unpin>>),
            encoder,
        }
    }

    pub fn split_borrow(
        &mut self,
    ) -> (
        &mut SyncWrapper<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
        &mut E,
    ) {
        (&mut self.sink, &mut self.encoder)
    }
}

impl<E: Encoder + Unpin> Sink<E::Message> for CodecSink<E> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: E::Message) -> Result<(), Self::Error> {
        self.encoder.start_send(&item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let (sink, codec) = self.split_borrow();
        let sink = Pin::new(sink).get_pin_mut();

        codec.poll_flush(sink, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
