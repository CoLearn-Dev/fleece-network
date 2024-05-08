use std::time::Duration;

use libp2p::{
    dcutr, identify,
    identity::Keypair,
    ping, relay, rendezvous,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
    upnp,
};

use super::codec;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub(super) identify: identify::Behaviour,
    pub(super) rendezvous: rendezvous::client::Behaviour,
    pub(super) request_response: request_response::Behaviour<codec::Codec>,
    pub(super) stream: libp2p_stream::Behaviour,
    pub(super) relay_client: relay::client::Behaviour,
    pub(super) upnp: upnp::tokio::Behaviour,
    pub(super) dcutr: dcutr::Behaviour,
    pub(super) ping: ping::Behaviour,
}

impl Behaviour {
    pub fn new(keypair: &Keypair, relay_behaviour: relay::client::Behaviour) -> Self {
        let codec = codec::Codec::default();
        let codec_protocol = core::iter::once((codec::Protocol::default(), ProtocolSupport::Full));
        Self {
            identify: identify::Behaviour::new(identify::Config::new(
                "/TODO/1.0.0".to_string(),
                keypair.public(),
            )),
            rendezvous: rendezvous::client::Behaviour::new(keypair.clone()),
            request_response: request_response::Behaviour::with_codec(
                codec,
                codec_protocol,
                request_response::Config::default(),
            ),
            stream: libp2p_stream::Behaviour::new(),
            relay_client: relay_behaviour,
            upnp: upnp::tokio::Behaviour::default(),
            dcutr: dcutr::Behaviour::new(keypair.public().to_peer_id()),
            ping: ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(60))),
        }
    }
}
