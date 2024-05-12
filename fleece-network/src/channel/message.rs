use std::{fmt::Debug, io};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub(crate) type OneshotSender<T> = oneshot::Sender<Result<T, io::Error>>;

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct InboundRequestId(pub u64);

impl From<OutboundRequestId> for InboundRequestId {
    fn from(value: OutboundRequestId) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub struct OutboundRequestId(pub u64);

impl From<InboundRequestId> for OutboundRequestId {
    fn from(value: InboundRequestId) -> Self {
        Self(value.0)
    }
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, Deserialize, Serialize)]
pub enum RequestId {
    Inbound(InboundRequestId),
    Outbound(OutboundRequestId),
}

impl Into<u64> for RequestId {
    fn into(self) -> u64 {
        match self {
            Self::Inbound(InboundRequestId(id)) => id,
            Self::Outbound(OutboundRequestId(id)) => id,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum OutboundMessage<Req, Resp> {
    Request(OutboundRequestId, Req),
    Response(InboundRequestId, Resp),
}

impl<Req, Resp> OutboundMessage<Req, Resp> {
    pub fn request_id(&self) -> RequestId {
        match self {
            Self::Request(id, _) => RequestId::Outbound(*id),
            Self::Response(id, _) => RequestId::Inbound(*id),
        }
    }
}
impl<Req, Resp> From<InboundMessage<Req, Resp>> for OutboundMessage<Req, Resp> {
    fn from(value: InboundMessage<Req, Resp>) -> Self {
        match value {
            InboundMessage::Request(id, request) => OutboundMessage::Request(id.into(), request),
            InboundMessage::Response(id, response) => {
                OutboundMessage::Response(id.into(), response)
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum InboundMessage<Req, Resp> {
    Request(InboundRequestId, Req),
    Response(OutboundRequestId, Resp),
}

impl<Req, Resp> InboundMessage<Req, Resp> {
    pub fn request_id(&self) -> RequestId {
        match self {
            Self::Request(id, _) => RequestId::Inbound(*id),
            Self::Response(id, _) => RequestId::Outbound(*id),
        }
    }
}

impl<Req, Resp> From<OutboundMessage<Req, Resp>> for InboundMessage<Req, Resp> {
    fn from(value: OutboundMessage<Req, Resp>) -> Self {
        match value {
            OutboundMessage::Request(id, request) => InboundMessage::Request(id.into(), request),
            OutboundMessage::Response(id, response) => {
                InboundMessage::Response(id.into(), response)
            }
        }
    }
}

#[derive(Debug)]
pub enum OutboundHandle<Req, Resp> {
    Request(OutboundRequestId, Req, OneshotSender<Resp>),
    Response(InboundRequestId, Resp, OneshotSender<()>),
}

impl<Req, Resp> OutboundHandle<Req, Resp> {
    pub fn split(self) -> (OutboundMessage<Req, Resp>, OutboundCallback<Resp>) {
        match self {
            OutboundHandle::Request(id, request, sender) => (
                OutboundMessage::Request(id, request),
                OutboundCallback::Request(sender),
            ),
            OutboundHandle::Response(id, response, sender) => (
                OutboundMessage::Response(id, response),
                OutboundCallback::Response(sender),
            ),
        }
    }
}

pub enum OutboundCallback<Resp> {
    Request(OneshotSender<Resp>),
    Response(OneshotSender<()>),
}
