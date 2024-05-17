use std::time::Duration;

use bytes::Bytes;
use libp2p::{identity, swarm, Multiaddr, PeerId, Swarm};
use tokio::sync::mpsc;
use tower::Service;

use crate::{channel::InboundHandle, error::Error, router::Router, transport::TransportBuilder};

use super::{
    behaviour::Behaviour,
    codec,
    eventloop::{Command, EventLoop},
    handler::Handler,
};

pub struct Peer {
    pub peer_id: PeerId,

    eventloop: EventLoop,

    router:
        Router<codec::Request, codec::Response, Error, Handler<codec::Request, codec::Response>>,
    request_receiver: mpsc::Receiver<InboundHandle<codec::Request, codec::Response>>,

    pub command_tx: mpsc::Sender<Command>,
}

impl Peer {
    pub fn new(
        center_addr: Multiaddr,
        center_peer_id: PeerId,
        self_addr: Multiaddr,
        handler: Handler<codec::Request, codec::Response>,
    ) -> Self {
        let keypair = identity::Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let transport_builder = TransportBuilder::new(keypair.clone())
            .with_tcp()
            .with_ws()
            .with_quic();
        let (transport_builder, relay_behaviour) = transport_builder.with_relay();
        let transport = transport_builder.build();
        let (behaviour, receiver) = Behaviour::new(&keypair, relay_behaviour);
        let swarm_config = swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(600));
        let mut swarm = Swarm::new(transport, behaviour, peer_id.clone(), swarm_config);

        // setup for direct connection
        swarm.listen_on(self_addr.clone()).unwrap();

        // setup for stream

        let (command_tx, command_rx) = mpsc::channel(128);
        let eventloop = EventLoop::new(
            swarm,
            command_tx.clone(),
            command_rx,
            center_addr.clone(),
            center_peer_id.clone(),
        );

        Self {
            peer_id,
            eventloop,
            router: Router::new(handler),
            request_receiver: receiver,
            command_tx,
        }
    }

    pub async fn run(mut self) {
        tokio::spawn(self.eventloop.run());
        loop {
            match self.request_receiver.recv().await {
                Some(handle) => {
                    let (_, request, sender) = handle.into_parts();
                    let future = self.router.call(request);
                    tokio::spawn(async move {
                        let response = future.await;
                        if response.is_err() {
                            sender
                                .send(codec::Response::new(
                                    String::from("error"),
                                    Bytes::default(),
                                ))
                                .unwrap();
                        } else {
                            sender.send(response.unwrap()).unwrap();
                        }
                    });
                }
                None => break,
            }
        }
    }
}
