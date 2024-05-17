use std::time::Duration;

use libp2p::{
    dcutr, identify, identity::Keypair, ping, relay, rendezvous, swarm::NetworkBehaviour, upnp,
};
use tokio::sync::mpsc;

use crate::channel::{self, InboundHandle};

use super::codec;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub(super) identify: identify::Behaviour,
    pub(super) rendezvous: rendezvous::client::Behaviour,
    pub(super) channel: channel::Behaviour<codec::Codec>,
    pub(super) relay_client: relay::client::Behaviour,
    pub(super) upnp: upnp::tokio::Behaviour,
    pub(super) dcutr: dcutr::Behaviour,
    pub(super) ping: ping::Behaviour,
}

impl Behaviour {
    pub fn new(
        keypair: &Keypair,
        relay_behaviour: relay::client::Behaviour,
    ) -> (
        Self,
        mpsc::Receiver<InboundHandle<codec::Request, codec::Response>>,
    ) {
        let codec = codec::Codec::default();
        let (sender, receiver) = mpsc::channel(128);
        (
            Self {
                identify: identify::Behaviour::new(identify::Config::new(
                    "/TODO/1.0.0".to_string(),
                    keypair.public(),
                )),
                rendezvous: rendezvous::client::Behaviour::new(keypair.clone()),
                channel: channel::Behaviour::new(
                    codec,
                    codec::Protocol::default(),
                    channel::Config::default(),
                    sender,
                ),
                relay_client: relay_behaviour,
                upnp: upnp::tokio::Behaviour::default(),
                dcutr: dcutr::Behaviour::new(keypair.public().to_peer_id()),
                ping: ping::Behaviour::new(
                    ping::Config::new().with_interval(Duration::from_secs(60)),
                ),
            },
            receiver,
        )
    }
}
