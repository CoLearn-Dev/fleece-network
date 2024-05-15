use std::time::Duration;

use libp2p::{identity, multiaddr::Protocol, swarm, Multiaddr, PeerId, Swarm};
use tokio::sync::mpsc;

use crate::{channel::InboundHandle, peer::eventloop::Command, transport::TransportBuilder};

use super::{behaviour::Behaviour, codec, eventloop::EventLoop};

pub struct Proxy {
    pub peer_id: PeerId,
    pub command_tx: mpsc::Sender<Command>,
    pub request_rx: mpsc::Receiver<InboundHandle<codec::Request, codec::Response>>,
}

impl Proxy {
    pub fn new(
        center_addr: Multiaddr,
        center_peer_id: PeerId,
        self_addr: Multiaddr,
    ) -> (Self, EventLoop) {
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let transport_builder = TransportBuilder::new(keypair.clone())
            .with_tcp()
            .with_ws()
            .with_quic();
        let (transport_builder, relay_behaviour) = transport_builder.with_relay();
        let transport = transport_builder.build();
        let (behaviour, request_rx) = Behaviour::new(&keypair, relay_behaviour);
        let swarm_config = swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(600));
        let mut swarm = Swarm::new(transport, behaviour, peer_id.clone(), swarm_config);

        // setup for direct connection
        swarm.listen_on(self_addr.clone()).unwrap();

        // setup for relay
        let relay_addr = center_addr
            .clone()
            .with(Protocol::P2pCircuit)
            .with(Protocol::P2p(peer_id));
        swarm.listen_on(relay_addr.clone()).unwrap();
        swarm.add_external_address(relay_addr.clone());

        // setup for stream

        let (command_tx, command_rx) = mpsc::channel(32);
        let eventloop = EventLoop::new(
            swarm,
            command_tx.clone(),
            command_rx,
            center_addr.clone(),
            center_peer_id.clone(),
        );

        (
            Self {
                peer_id,
                command_tx,
                request_rx,
            },
            eventloop,
        )
    }

    pub fn into_parts(
        self,
    ) -> (
        PeerId,
        mpsc::Sender<Command>,
        mpsc::Receiver<InboundHandle<codec::Request, codec::Response>>,
    ) {
        (self.peer_id, self.command_tx, self.request_rx)
    }
}
