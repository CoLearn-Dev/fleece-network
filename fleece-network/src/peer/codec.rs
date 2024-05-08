use std::io;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use libp2p::request_response;
use serde::{Deserialize, Serialize};

use crate::router::Routable;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
pub struct Codec {
    /// Necessary in order to avoid DoS attacks.
    max_response_size: usize,
}

impl Default for Codec {
    fn default() -> Self {
        Self {
            max_response_size: 2 * 8192 * 262 * 1024,
        }
    }
}

#[async_trait]
impl request_response::Codec for Codec {
    type Protocol = Protocol;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        socket: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut request = Vec::new();
        socket
            .take(self.max_response_size as u64)
            .read_to_end(&mut request)
            .await?;
        bincode::deserialize(&request).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        socket: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut response = Vec::new();
        socket
            .take(self.max_response_size as u64)
            .read_to_end(&mut response)
            .await?;

        bincode::deserialize(&response).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        socket: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        let encoded_data =
            bincode::serialize(&req).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        socket.write_all(&encoded_data).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        socket: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        let encoded_data =
            bincode::serialize(&res).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        socket.write_all(&encoded_data).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Protocol;

impl AsRef<str> for Protocol {
    fn as_ref(&self) -> &str {
        "/fleece/1.0.0"
    }
}

impl Default for Protocol {
    fn default() -> Self {
        Self {}
    }
}
