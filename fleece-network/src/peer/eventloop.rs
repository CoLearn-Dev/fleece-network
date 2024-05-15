use std::collections::{hash_map::Entry, HashMap};

use instant::Duration;
use libp2p::{
    futures::StreamExt,
    multiaddr::Protocol,
    rendezvous::{self, Cookie},
    swarm::{dial_opts::DialOpts, SwarmEvent},
    Multiaddr, PeerId, Swarm,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{self, Interval},
};
use tracing::{debug, info, warn};

use crate::{
    channel::{self, OneshotSender},
    error::Error,
};

use super::{
    behaviour::{Behaviour, BehaviourEvent},
    codec,
};

type Medium<T> = oneshot::Sender<Result<T, Error>>;

pub struct EventLoop {
    swarm: Swarm<Behaviour>,

    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,

    center_addr: Multiaddr,
    center_peer_id: PeerId,

    interval: Interval,
    last_cookie: Option<Cookie>,

    pending_dials: HashMap<PeerId, Medium<()>>,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<Behaviour>,
        command_tx: mpsc::Sender<Command>,
        command_rx: mpsc::Receiver<Command>,
        center_addr: Multiaddr,
        center_peer_id: PeerId,
    ) -> Self {
        Self {
            swarm,
            command_tx,
            command_rx,
            center_addr,
            center_peer_id,
            interval: time::interval(Duration::from_secs(1)),
            last_cookie: None,
            pending_dials: Default::default(),
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
                _ = self.interval.tick() => {
                    self.command_tx
                        .send(Command::Discover {
                            namespace: String::from("fleece"),
                        })
                        .await
                        .unwrap();
                    if !self.swarm.is_connected(&self.center_peer_id) {
                        self.command_tx
                            .send(Command::Dial {
                                peer_id: self.center_peer_id,
                                peer_addr: Some(self.center_addr.clone()),
                                sender: None,
                            })
                            .await
                            .unwrap();
                    }
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
                    channel::behaviour::Event::MissedResponse {
                        request_id,
                        response: _,
                    } => {
                        warn!("Missed response: {:?}", request_id);
                    }
                    channel::behaviour::Event::Failure { peer_id, failure } => {
                        info!("Channel failure {:?}: {:?}", peer_id, failure);
                    }
                },
            },
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if peer_id == self.center_peer_id {
                    // setup for rendzvous
                    let namespace = String::from("fleece");
                    self.command_tx
                        .send(Command::Register {
                            namespace: namespace.clone(),
                        })
                        .await
                        .unwrap();

                    // setup for relay
                    let relay_addr = self
                        .center_addr
                        .clone()
                        .with(Protocol::P2pCircuit)
                        .with(Protocol::P2p(peer_id));
                    self.swarm.listen_on(relay_addr.clone()).unwrap();
                    self.swarm.add_external_address(relay_addr.clone());
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
            SwarmEvent::NewListenAddr {
                listener_id: _,
                address,
            } => {
                info!("New listen address: {}", address);
            }
            SwarmEvent::ExpiredListenAddr { .. } => {}
            SwarmEvent::ListenerClosed { .. } => {}
            SwarmEvent::ListenerError { .. } => {}
            SwarmEvent::Dialing { .. } => {}
            SwarmEvent::NewExternalAddrCandidate { .. } => {}
            SwarmEvent::ExternalAddrConfirmed { address } => {
                info!("External address confirmed: {}", address);
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
}
