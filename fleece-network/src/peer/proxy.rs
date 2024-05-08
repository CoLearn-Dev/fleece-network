use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use chrono::Local;
use crossbeam_channel::{self, unbounded};
use futures::Future;
use libp2p::{identity, multiaddr::Protocol, swarm, Multiaddr, PeerId, Swarm};
use libp2p_request_response::InboundRequestId;
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
};

use crate::{error::Error, peer::eventloop::Command, transport::TransportBuilder};

use super::{
    behaviour::Behaviour,
    codec,
    eventloop::{Event, EventLoop},
};

type Medium<T> = oneshot::Sender<Result<T, Error>>;

pub struct Proxy {
    pub peer_id: PeerId,
    pub instruct_tx: mpsc::Sender<Instruct>,
    pub message_rx: crossbeam_channel::Receiver<(InboundRequestId, codec::Request)>,
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
        let (instruct_tx, mut instruct_rx) = mpsc::channel(32);
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
        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        tokio::spawn(eventloop.run());
        let future = Box::pin(async move {
            // initial dial
            tokio::time::sleep(Duration::from_secs(1)).await;
            command_tx
                .send(Command::Dial {
                    peer_id: center_peer_id,
                    peer_addr: Some(center_addr),
                    sender: None,
                })
                .await
                .unwrap();
            loop {
                select! {
                    event = event_rx.recv() => match event {
                        Some(event) => match event {
                            Event::Request {
                                request_id,
                                request,
                                channel,
                            } => {
                                pending_requests.lock().await.insert(request_id, channel);
                                message_tx.send((request_id, request)).unwrap();
                            }
                        },
                        None => break,
                    },

                    instruct = instruct_rx.recv() => {
                        match instruct {
                            Some(instruct) => match instruct {
                                Instruct::Request {
                                    peer_id,
                                    request,
                                    sender,
                                } => {
                                    let now = Local::now();
                                    println!(
                                        "Current time (microseconds) B: {}",
                                        now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                                    );
                                    command_tx.send(Command::Request {
                                        peer_id,
                                        request,
                                        sender,
                                    }).await.unwrap();
                                },
                                Instruct::Response {
                                    request_id,
                                    response,
                                    sender,
                                } => {
                                    let channel = pending_requests.lock().await.remove(&request_id).unwrap();
                                    command_tx.send(Command::Response {
                                        request_id,
                                        channel,
                                        response,
                                        sender,
                                    }).await.unwrap();
                                },
                            },
                            None => break,
                        }
                    },
                }
            }
        });

        (
            Self {
                peer_id,
                instruct_tx,
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
        request_id: InboundRequestId,
        response: codec::Response,
        sender: Medium<()>,
    },
}
