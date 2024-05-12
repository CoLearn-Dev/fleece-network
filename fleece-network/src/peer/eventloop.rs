use std::collections::{hash_map::Entry, HashMap};

use futures_timer::Delay;
use instant::Duration;
use libp2p::{
    futures::StreamExt,
    rendezvous::{self, Cookie},
    swarm::{dial_opts::DialOpts, SwarmEvent},
    Multiaddr, PeerId, Swarm,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    channel::{self, InboundRequestId, OneshotSender},
    error::Error,
};

use super::{
    behaviour::{Behaviour, BehaviourEvent},
    codec,
};

type Medium<T> = oneshot::Sender<Result<T, Error>>;

pub(super) struct EventLoop {
    swarm: Swarm<Behaviour>,

    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,

    center_addr: Multiaddr,
    center_peer_id: PeerId,

    delay: Delay,
    last_cookie: Option<Cookie>,

    pending_dials: HashMap<PeerId, Medium<()>>,
    pending_requests: HashMap<channel::OutboundRequestId, Medium<codec::Response>>,
    pending_responses: HashMap<channel::InboundRequestId, Medium<()>>,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<Behaviour>,
        command_tx: mpsc::Sender<Command>,
        command_rx: mpsc::Receiver<Command>,
        event_tx: mpsc::Sender<Event>,
        center_addr: Multiaddr,
        center_peer_id: PeerId,
    ) -> Self {
        Self {
            swarm,
            command_tx,
            command_rx,
            event_tx,
            center_addr,
            center_peer_id,
            delay: Delay::new(Duration::from_secs(1)),
            last_cookie: None,
            pending_dials: Default::default(),
            pending_requests: Default::default(),
            pending_responses: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                command = self.command_rx.recv() => {
                    match command {
                        Some(command) => self.handle_command(command).await,
                        None => break,
                    }
                }
                event = self.swarm.select_next_some() => self.handle_event(event).await,
                _ = &mut self.delay => {
                    self.delay.reset(Duration::from_secs(1));
                    self.command_tx
                        .send(Command::Discover {
                            namespace: String::from("fleece"),
                        })
                        .await
                        .unwrap();
                }
            }
        }
    }

    async fn handle_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(event) => match event {
                BehaviourEvent::Identify(_) => {}
                BehaviourEvent::Rendezvous(event) => match event {
                    rendezvous::client::Event::Discovered {
                        registrations,
                        cookie,
                        ..
                    } => {
                        self.last_cookie.replace(cookie);
                        for registration in registrations {
                            for address in registration.record.addresses() {
                                info!("Find: {:?}", address);
                            }
                        }

                        // for registration in registrations {
                        //     for address in registration.record.addresses() {
                        //         let peer = registration.record.peer_id();
                        //         if peer == *self.swarm.local_peer_id() {
                        //             continue;
                        //         }

                        //         let p2p_suffix = multiaddr::Protocol::P2p(peer);
                        //         let address_with_p2p = if !address
                        //             .ends_with(&Multiaddr::empty().with(p2p_suffix.clone()))
                        //         {
                        //             address.clone().with(p2p_suffix)
                        //         } else {
                        //             address.clone()
                        //         };
                        //         info!("Dialing: {:?}", address_with_p2p);
                        //         println!("Dialing: {:?}", address_with_p2p);
                        //         self.swarm.dial(address_with_p2p).unwrap();
                        //     }
                        // }
                    }
                    rendezvous::client::Event::DiscoverFailed { .. } => {}
                    rendezvous::client::Event::Registered { .. } => {}
                    rendezvous::client::Event::RegisterFailed { .. } => {}
                    rendezvous::client::Event::Expired { .. } => {}
                },
                BehaviourEvent::RelayClient(_) => {}
                BehaviourEvent::Upnp(_) => {}
                BehaviourEvent::Dcutr(_) => {}
                BehaviourEvent::Ping(event) => {
                    if let Ok(duration) = event.result {
                        info!("Ping {:?}: {:?}", event.peer, duration);
                        self.swarm.behaviour_mut().channel.update_rtt(
                            &event.peer,
                            event.connection,
                            duration,
                        );
                    }
                }
                BehaviourEvent::Channel(event) => match event {
                    channel::behaviour::Event::Request {
                        peer_id,
                        request_id,
                        request,
                    } => {
                        self.event_tx
                            .send(Event::Request {
                                peer_id,
                                request_id,
                                request,
                            })
                            .await
                            .unwrap();
                    }
                    channel::behaviour::Event::MissedResponse {
                        request_id,
                        response,
                    } => todo!(),
                    channel::behaviour::Event::Failure { peer_id, failure } => {
                        info!("Channel failure {:?}: {:?}", peer_id, failure);
                    }
                },
            },
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if peer_id == self.center_peer_id {
                    let namespace = String::from("fleece");
                    self.command_tx
                        .send(Command::Register {
                            namespace: namespace.clone(),
                        })
                        .await
                        .unwrap();
                    self.command_tx
                        .send(Command::Discover {
                            namespace: namespace.clone(),
                        })
                        .await
                        .unwrap();
                }

                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        sender.send(Ok(())).unwrap();
                    }
                }
            }
            SwarmEvent::ConnectionClosed { .. } => {}
            SwarmEvent::IncomingConnection { .. } => {}
            SwarmEvent::IncomingConnectionError { .. } => {}
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                info!("Outgoing connection error: {:?}", error);
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dials.remove(&peer_id) {
                        sender.send(Err(Error::from(error))).unwrap();
                    }
                }
            }
            SwarmEvent::NewListenAddr { .. } => {}
            SwarmEvent::ExpiredListenAddr { .. } => {}
            SwarmEvent::ListenerClosed { .. } => {}
            SwarmEvent::ListenerError { .. } => {}
            SwarmEvent::Dialing { .. } => {}
            SwarmEvent::NewExternalAddrCandidate { .. } => {}
            SwarmEvent::ExternalAddrConfirmed { address } => {
                info!("External address confirmed: {}", address);
                println!("External address confirmed: {}", address);
            }
            SwarmEvent::ExternalAddrExpired { .. } => {}
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                info!("New external address of peer {}: {}", peer_id, address);
            }
            _ => todo!(),
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if let Entry::Vacant(entry) = self.pending_dials.entry(peer_id) {
                    let dial: DialOpts = if let Some(peer_addr) = peer_addr {
                        peer_addr.into()
                    } else {
                        peer_id.into()
                    };
                    if let Some(sender) = sender {
                        match self.swarm.dial(dial) {
                            Ok(_) => {
                                entry.insert(sender);
                            }
                            Err(err) => sender.send(Err(Error::from(err))).unwrap(),
                        }
                    } else {
                        let result = self.swarm.dial(dial);
                        info!("Dial result: {:?}", result);
                    }
                }
            }
            Command::Register { namespace } => {
                debug!("Registering namespace: {}", namespace);
                self.swarm
                    .behaviour_mut()
                    .rendezvous
                    .register(
                        rendezvous::Namespace::new(namespace).unwrap(),
                        self.center_peer_id,
                        None,
                    )
                    .unwrap();
            }
            Command::Unregister { namespace } => {
                debug!("Unregistering namespace: {}", namespace);
                self.swarm.behaviour_mut().rendezvous.unregister(
                    rendezvous::Namespace::new(namespace).unwrap(),
                    self.center_peer_id,
                );
            }
            Command::Discover { namespace } => {
                if self.swarm.is_connected(&self.center_peer_id) {
                    debug!("Discovering namespace: {}", namespace);
                    self.swarm.behaviour_mut().rendezvous.discover(
                        Some(rendezvous::Namespace::new(namespace).unwrap()),
                        self.last_cookie.clone(),
                        None,
                        self.center_peer_id,
                    );
                }
            }
            Command::Request {
                peer_id,
                request,
                sender,
            } => {
                self.swarm
                    .behaviour_mut()
                    .channel
                    .send_request(&peer_id, request, sender);
            }
            Command::Response {
                peer_id,
                request_id,
                response,
                sender,
            } => {
                self.swarm
                    .behaviour_mut()
                    .channel
                    .send_response(&peer_id, request_id, response, sender);
            }
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Dial {
        peer_id: PeerId,
        peer_addr: Option<Multiaddr>,
        sender: Option<Medium<()>>,
    },
    Register {
        namespace: String,
    },
    Unregister {
        namespace: String,
    },
    Discover {
        namespace: String,
    },
    Request {
        peer_id: PeerId,
        request: codec::Request,
        sender: OneshotSender<codec::Response>,
    },
    Response {
        peer_id: PeerId,
        request_id: channel::InboundRequestId,
        response: codec::Response,
        sender: OneshotSender<()>,
    },
}

pub(super) enum Event {
    Request {
        peer_id: PeerId,
        request_id: InboundRequestId,
        request: codec::Request,
    },
}
