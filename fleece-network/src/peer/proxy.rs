use std::{io, pin::Pin, time::Duration};

use crossbeam_channel::{self, unbounded};
use futures::Future;
use libp2p::{identity, multiaddr::Protocol, swarm, Multiaddr, PeerId, Swarm};
use swarm::ConnectionId;
use tokio::{
    select,
    sync::{mpsc, oneshot},
};

use crate::{channel::InboundRequestId, peer::eventloop::Command, transport::TransportBuilder};

use super::{
    behaviour::Behaviour,
    codec,
    eventloop::{Event, EventLoop},
};

type Medium<T> = oneshot::Sender<Result<T, io::Error>>;

pub struct Proxy {
    pub peer_id: PeerId,
    pub command_tx: mpsc::Sender<Command>,
    pub message_rx:
        crossbeam_channel::Receiver<(PeerId, ConnectionId, InboundRequestId, codec::Request)>,
}

impl Proxy {
    pub fn new(
        center_addr: Multiaddr,
        center_peer_id: PeerId,
        self_addr: Multiaddr,
    ) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let transport_builder = TransportBuilder::new(keypair.clone())
            .with_tcp()
            .with_ws()
            .with_quic();
        let (transport_builder, relay_behaviour) = transport_builder.with_relay();
        let transport = transport_builder.build();
        let behaviour = Behaviour::new(&keypair, relay_behaviour);
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
        let (event_tx, mut event_rx) = mpsc::channel(32);
        let (message_tx, message_rx) = unbounded();
        let eventloop = EventLoop::new(
            swarm,
            command_tx.clone(),
            command_rx,
            event_tx,
            center_addr.clone(),
            center_peer_id.clone(),
        );

        tokio::spawn(eventloop.run());
        let future = Box::pin(async move {
            loop {
                select! {
                    event = event_rx.recv() => match event {
                        Some(event) => match event {
                            Event::Request { peer_id, connection_id, request_id, request } => {
                                message_tx.send((peer_id, connection_id, request_id, request)).unwrap();
                            },
                        },
                        None => break,
                    },
                }
            }
        });

        (
            Self {
                peer_id,
                command_tx,
                message_rx,
            },
            future,
        )
    }
}

pub enum Instruct {
    Request {
        peer_id: PeerId,
        request: codec::Request,
        sender: Medium<codec::Response>,
    },
    Response {
        peer_id: PeerId,
        request_id: InboundRequestId,
        response: codec::Response,
        sender: Medium<()>,
    },
}
