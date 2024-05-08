use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::router::Routable;

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub id: u64,
    pub route: String,
    pub payload: Bytes,
}

impl Request {
    pub fn new(id: u64, route: String, payload: Bytes) -> Self {
        Self { id, route, payload }
    }
}

impl Routable for Request {
    fn route(&self) -> &str {
        &self.route
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub id: u64,
    pub status: String,
    pub payload: Bytes,
}

impl Response {
    pub fn new(id: u64, status: String, payload: Bytes) -> Self {
        Self {
            id,
            status,
            payload,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Request(Request),
    Response(Response),
    ResponseSent(u64),
}
