use std::{fmt::Debug, io};

use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

pub(crate) type OneshotSender<T> = oneshot::Sender<Result<T, io::Error>>;
pub(crate) type DirectSender<T> = oneshot::Sender<T>;

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

impl<Req, Resp> From<OutboundHandle<Req, Resp>> for OutboundMessage<Req, Resp> {
    fn from(value: OutboundHandle<Req, Resp>) -> Self {
        OutboundMessage::Request(value.id, value.request)
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

impl<Req, Resp> From<InboundHandle<Req, Resp>> for InboundMessage<Req, Resp> {
    fn from(value: InboundHandle<Req, Resp>) -> Self {
        InboundMessage::Request(value.id, value.request)
    }
}

#[derive(Debug)]
pub struct OutboundHandle<Req, Resp> {
    pub id: OutboundRequestId,
    pub request: Req,
    pub sender: OneshotSender<Resp>,
}

impl<Req, Resp> OutboundHandle<Req, Resp> {
    pub fn new(id: OutboundRequestId, request: Req, sender: OneshotSender<Resp>) -> Self {
        Self {
            id,
            request,
            sender,
        }
    }

    pub fn split(self) -> (OutboundMessage<Req, Resp>, OneshotSender<Resp>) {
        (OutboundMessage::Request(self.id, self.request), self.sender)
    }
}

#[derive(Debug)]
pub struct InboundHandle<Req, Resp> {
    pub id: InboundRequestId,
    pub request: Req,
    pub sender: DirectSender<Resp>,
}

impl<Req, Resp> InboundHandle<Req, Resp> {
    pub fn new(id: InboundRequestId, request: Req, sender: DirectSender<Resp>) -> Self {
        Self {
            id,
            request,
            sender,
        }
    }

    pub fn split(self) -> (InboundMessage<Req, Resp>, DirectSender<Resp>) {
        (InboundMessage::Request(self.id, self.request), self.sender)
    }

    pub fn into_parts(self) -> (InboundRequestId, Req, DirectSender<Resp>) {
        (self.id, self.request, self.sender)
    }
}

#[derive(Debug)]
pub enum OutboundHandleFake<Req, Resp> {
    Request(OutboundRequestId, Req, OneshotSender<Resp>),
    Response(InboundRequestId, Resp, OneshotSender<()>),
}

impl<Req, Resp> OutboundHandleFake<Req, Resp> {
    pub fn split(self) -> (OutboundMessage<Req, Resp>, OutboundCallbackFake<Resp>) {
        match self {
            OutboundHandleFake::Request(id, request, sender) => (
                OutboundMessage::Request(id, request),
                OutboundCallbackFake::Request(sender),
            ),
            OutboundHandleFake::Response(id, response, sender) => (
                OutboundMessage::Response(id, response),
                OutboundCallbackFake::Response(sender),
            ),
        }
    }
}

pub enum OutboundCallbackFake<Resp> {
    Request(OneshotSender<Resp>),
    Response(OneshotSender<()>),
}
