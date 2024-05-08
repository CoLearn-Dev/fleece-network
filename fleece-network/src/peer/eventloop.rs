use std::collections::{hash_map::Entry, HashMap};

use chrono::Local;
use libp2p::{
    futures::StreamExt,
    rendezvous::{self, Cookie},
    request_response::{self, InboundRequestId, OutboundRequestId, ResponseChannel},
    swarm::{dial_opts::DialOpts, SwarmEvent},
    Multiaddr, PeerId, Swarm,
};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::error::Error;

use super::{
    behaviour::{Behaviour, BehaviourEvent},
    codec, raw,
};

type Medium<T> = oneshot::Sender<Result<T, Error>>;

pub(super) struct EventLoop {
    swarm: Swarm<Behaviour>,

    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,

    center_addr: Multiaddr,
    center_peer_id: PeerId,

    last_cookie: Option<Cookie>,

    pending_dials: HashMap<PeerId, Medium<()>>,
    codec_pending_requests: HashMap<OutboundRequestId, Medium<codec::Response>>,
    codec_pending_responses: HashMap<InboundRequestId, Medium<()>>,
    raw_pending_requests: HashMap<u64, Medium<raw::Response>>,
    raw_pending_responses: HashMap<u64, Medium<()>>,
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
            last_cookie: None,
            pending_dials: Default::default(),
            codec_pending_requests: Default::default(),
            codec_pending_responses: Default::default(),
            raw_pending_requests: Default::default(),
            raw_pending_responses: Default::default(),
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
                BehaviourEvent::RequestResponse(event) => match event {
                    request_response::Event::Message { message, .. } => match message {
                        request_response::Message::Request {
                            request_id,
                            request,
                            channel,
                        } => {
                            let now = Local::now();
                            println!(
                                "Current time (microseconds) E: {}",
                                now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                            );
                            self.event_tx
                                .send(Event::Request {
                                    request_id,
                                    request,
                                    channel,
                                })
                                .await
                                .unwrap();
                        }
                        request_response::Message::Response {
                            request_id,
                            response,
                        } => {
                            if let Some(sender) = self.codec_pending_requests.remove(&request_id) {
                                let now = Local::now();
                                println!(
                                    "Current time (microseconds) D: {}",
                                    now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                                );
                                sender.send(Ok(response)).unwrap();
                            }
                        }
                    },
                    request_response::Event::OutboundFailure {
                        request_id, error, ..
                    } => {
                        if let Some(sender) = self.codec_pending_requests.remove(&request_id) {
                            sender.send(Err(Error::from(error))).unwrap();
                        }
                    }
                    request_response::Event::InboundFailure { .. } => {}
                    request_response::Event::ResponseSent { request_id, .. } => {
                        if let Some(sender) = self.codec_pending_responses.remove(&request_id) {
                            sender.send(Ok(())).unwrap();
                        }
                    }
                },
                BehaviourEvent::RelayClient(event) => {}
                BehaviourEvent::Upnp(_) => {}
                BehaviourEvent::Dcutr(event) => {
                    info!("dcutr: {:?}", event);
                }
                BehaviourEvent::Ping(event) => {}
                BehaviourEvent::Stream(_) => {}
            },
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                info!("Connection established {:?}", peer_id);
                println!("Connection established {:?}", peer_id);

                if peer_id == self.center_peer_id {
                    let namespace = "TODO".to_string();
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
                info!("Registering namespace: {}", namespace);
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
                info!("Unregistering namespace: {}", namespace);
                self.swarm.behaviour_mut().rendezvous.unregister(
                    rendezvous::Namespace::new(namespace).unwrap(),
                    self.center_peer_id,
                );
            }
            Command::Discover { namespace } => {
                info!("Discovering namespace: {}", namespace);
                self.swarm.behaviour_mut().rendezvous.discover(
                    Some(rendezvous::Namespace::new(namespace).unwrap()),
                    self.last_cookie.clone(),
                    None,
                    self.center_peer_id,
                );
            }
            Command::Request {
                peer_id,
                request,
                sender,
            } => {
                let now = Local::now();
                println!(
                    "Current time (microseconds) C: {}",
                    now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                );
                let req_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer_id, request);
                self.codec_pending_requests.insert(req_id, sender);
            }
            Command::Response {
                request_id,
                channel,
                response,
                sender,
            } => {
                let now = Local::now();
                println!(
                    "Current time (microseconds): {}",
                    now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
                );
                let result = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, response);
                if result.is_err() {
                    sender.send(Err(Error::ConnectionError)).unwrap();
                } else {
                    self.codec_pending_responses.insert(request_id, sender);
                }
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
        sender: Medium<codec::Response>,
    },
    Response {
        request_id: InboundRequestId,
        channel: ResponseChannel<codec::Response>,
        response: codec::Response,
        sender: Medium<()>,
    },
}

pub(super) enum Event {
    Request {
        request_id: InboundRequestId,
        request: codec::Request,
        channel: ResponseChannel<codec::Response>,
    },
}
