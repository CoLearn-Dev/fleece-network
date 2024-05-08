use libp2p::{request_response, swarm};
use thiserror;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Routing error")]
    RoutingError,

    #[error("Dialing error")]
    DialingError(#[from] swarm::DialError),

    #[error("Connection error")]
    ConnectionError,

    #[error("Outbound failure")]
    OutboundFailure(#[from] request_response::OutboundFailure),
}
