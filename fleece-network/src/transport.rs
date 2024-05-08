use std::marker::PhantomData;

use futures::future::Either;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed, upgrade::Version},
    dns,
    identity::Keypair,
    noise, quic, relay, tcp, yamux, PeerId, Transport,
};
use libp2p_mplex::MplexConfig;

pub struct TransportBuilder {
    keypair: Keypair,
    transport: Option<Boxed<(PeerId, StreamMuxerBox)>>,
}

impl TransportBuilder {
    pub fn new(keypair: Keypair) -> Self {
        Self {
            keypair,
            transport: None,
        }
    }

    pub fn with_transport(mut self, other: Boxed<(PeerId, StreamMuxerBox)>) -> Self {
        if let Some(transport) = self.transport {
            self.transport = Some(
                transport
                    .or_transport(other)
                    .map(|either, _| match either {
                        Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                        Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                    })
                    .boxed()
                    .boxed(),
            );
        } else {
            self.transport = Some(other);
        }

        self
    }

    pub fn with_tcp(self) -> Self {
        let noise_config = noise::Config::new(&self.keypair).unwrap();
        let multiplex_config = {
            let mplex_config = MplexConfig::default();
            let yamux_config = yamux::Config::default();
            libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
        };
        // `port_reuse` option conflicts with autonat behaviour
        let tcp = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true)).boxed();
        let tcp = tcp
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(multiplex_config)
            .boxed();
        self.with_transport(tcp)
    }

    pub fn with_quic(self) -> Self {
        let quic = quic::tokio::Transport::new(quic::Config::new(&self.keypair))
            .map(|(peer_id, connection), _| (peer_id, StreamMuxerBox::new(connection)))
            .boxed();
        self.with_transport(quic)
    }

    pub fn with_ws(self) -> Self {
        let noise_config = noise::Config::new(&self.keypair).unwrap();
        let multiplex_config = {
            let mplex_config = MplexConfig::default();
            let yamux_config = yamux::Config::default();
            libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
        };
        let tcp = tcp::tokio::Transport::new(tcp::Config::default().port_reuse(true).nodelay(true))
            .boxed();
        let ws = libp2p::websocket::WsConfig::new(tcp)
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(multiplex_config)
            .boxed();
        self.with_transport(ws)
    }

    pub fn with_relay(self) -> (Self, relay::client::Behaviour) {
        let noise_config = noise::Config::new(&self.keypair).unwrap();
        let multiplex_config = {
            let mplex_config = MplexConfig::default();
            let yamux_config = yamux::Config::default();
            libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
        };
        let (relay_transport, relay_behaviour) =
            libp2p::relay::client::new(self.keypair.public().to_peer_id());
        let relay_transport = relay_transport
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(multiplex_config)
            .boxed();
        (self.with_transport(relay_transport), relay_behaviour)
    }

    pub fn with_dns(mut self) -> Self {
        if let Some(transport) = self.transport {
            self.transport = Some(dns::tokio::Transport::system(transport).unwrap().boxed());
        }
        self
    }

    pub fn build(self) -> Boxed<(PeerId, StreamMuxerBox)> {
        self.transport.unwrap()
    }
}
