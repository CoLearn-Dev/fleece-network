use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    channel::{
        self, codec,
        message::{InboundMessage, OutboundMessage},
    },
    router::Routable,
    utils::chunk::{ChunkReader, ChunkWriter},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub route: String,
    pub payload: Bytes,
}

impl Request {
    pub fn new(route: String, payload: Bytes) -> Self {
        Self { route, payload }
    }
}

impl Routable for Request {
    fn route(&self) -> &str {
        &self.route
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub status: String,
    pub payload: Bytes,
}

impl Response {
    pub fn new(status: String, payload: Bytes) -> Self {
        Self { status, payload }
    }
}

#[derive(Debug, Clone)]
pub struct Codec {}

impl Default for Codec {
    fn default() -> Self {
        Self {}
    }
}

impl channel::Codec for Codec {
    type Protocol = Protocol;
    type Request = Request;
    type Response = Response;
    type Encoder = Encoder;
    type Decoder = Decoder;

    fn new_decoder(&self) -> Decoder {
        Decoder {
            read_phase: ReadPhase::Header(ChunkReader::new(8)),
        }
    }

    fn new_encoder(&self) -> Encoder {
        Encoder {}
    }
}

#[derive(Debug)]
pub struct Decoder {
    read_phase: ReadPhase,
}

impl codec::Decoder for Decoder {
    type Message = InboundMessage<Request, Response>;

    fn poll_read(
        &mut self,
        mut reader: Pin<&mut (impl AsyncRead + Unpin + Send)>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<InboundMessage<Request, Response>>> {
        loop {
            let reader = reader.as_mut();
            match &mut self.read_phase {
                ReadPhase::Header(chunk_reader) => match chunk_reader.poll_read(reader, cx) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {
                            let len = u64::from_be_bytes(
                                chunk_reader.buffer[..8].try_into().expect("invalid header"),
                            );
                            info!("Try to read {:?} bytes", len);
                            self.read_phase = ReadPhase::Payload(ChunkReader::new(len as usize));
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    },
                    Poll::Pending => return Poll::Pending,
                },
                ReadPhase::Payload(chunk_reader) => match chunk_reader.poll_read(reader, cx) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {
                            let message: OutboundMessage<Request, Response> =
                                bincode::deserialize(&chunk_reader.buffer[..]).unwrap();
                            self.read_phase = ReadPhase::Header(ChunkReader::new(8));
                            return Poll::Ready(Ok(message.into()));
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    },
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

#[derive(Debug)]
pub struct Encoder {}

impl codec::Encoder for Encoder {
    type Message = OutboundMessage<Request, Response>;

    fn poll_write(
        &mut self,
        mut writer: Pin<&mut (impl AsyncWrite + Unpin + Send)>,
        payload: &OutboundMessage<Request, Response>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let writer = writer.as_mut();
        let payload = bincode::serialize(payload).unwrap();
        let size = (payload.len() as u64).to_be_bytes();
        let mut buffer = BytesMut::with_capacity(8 + payload.len());
        buffer.extend_from_slice(&size);
        buffer.extend_from_slice(&payload);
        let mut chunk_writer = ChunkWriter::new(buffer.freeze());
        loop {
            match chunk_writer.poll_write(writer, cx) {
                Poll::Ready(result) => match result {
                    Ok(_) => return Poll::Ready(Ok(())),
                    Err(e) => return Poll::Ready(Err(e)),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug)]
enum ReadPhase {
    Header(ChunkReader),
    Payload(ChunkReader),
}

#[derive(Clone, Debug)]
pub struct Protocol;

impl AsRef<str> for Protocol {
    fn as_ref(&self) -> &str {
        "/fleece/channel/1.0.0"
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self {}
    }
}
