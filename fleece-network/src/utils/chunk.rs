use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite};
use tracing::info;

#[derive(Debug)]
pub struct ChunkReader {
    pub buffer: BytesMut,
    cursor: usize,
}

impl ChunkReader {
    pub fn new(size: usize) -> Self {
        let mut buffer = BytesMut::with_capacity(size);
        unsafe {
            buffer.set_len(size);
        }

        Self { buffer, cursor: 0 }
    }

    pub fn poll_read(
        &mut self,
        mut reader: Pin<&mut (impl AsyncRead + Unpin + Send)>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        loop {
            let reader = reader.as_mut();
            if self.cursor == self.buffer.len() {
                return Poll::Ready(Ok(()));
            }

            match reader.poll_read(cx, &mut self.buffer[self.cursor..]) {
                Poll::Ready(len) => match len {
                    Ok(len) => {
                        self.cursor += len;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Debug)]
pub struct ChunkWriter {
    pub buffer: Bytes,
    cursor: usize,
}

impl ChunkWriter {
    pub fn new(buffer: Bytes) -> Self {
        Self { buffer, cursor: 0 }
    }

    pub fn poll_write(
        &mut self,
        mut writer: Pin<&mut (impl AsyncWrite + Unpin + Send)>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        loop {
            let writer = writer.as_mut();
            if self.cursor == self.buffer.len() {
                info!("Write {:?} bytes", self.buffer.len());
                return writer.poll_flush(cx);
            }

            match writer.poll_write(cx, &self.buffer[self.cursor..]) {
                Poll::Ready(len) => match len {
                    Ok(len) => {
                        self.cursor += len;
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                },
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
