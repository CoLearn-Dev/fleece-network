use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    io,
    task::Poll,
};

use instant::Duration;
use libp2p::{core::Endpoint, Multiaddr, PeerId};
use libp2p_swarm::{
    dial_opts::DialOpts, ConnectionClosed, ConnectionDenied, ConnectionHandler, ConnectionId,
    DialFailure, FromSwarm, NetworkBehaviour, NotifyHandler, PeerAddresses, THandler,
    THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use tracing::info;

use super::{
    codec::Codec,
    handler::{self, ChannelFailure, Handler},
    message::{InboundRequestId, OutboundRequestId},
    Config, OneshotSender, OutboundHandle,
};

pub struct Behaviour<C: Codec> {
    codec: C,
    protocol: C::Protocol,
    config: Config,

    request_id: u64,
    pending_outbound_handles: HashMap<PeerId, Vec<OutboundHandle<C::Request, C::Response>>>,
    pending_events:
        VecDeque<ToSwarm<Event<C::Request, C::Response>, OutboundHandle<C::Request, C::Response>>>,
    connected_peers: HashMap<PeerId, Vec<Connection>>,
    addresses: PeerAddresses,
}

impl<C> Behaviour<C>
where
    C: Codec + Send + Debug + Clone + Unpin + 'static,
{
    pub fn new(codec: C, protocol: C::Protocol, config: Config) -> Self {
        Self {
            codec,
            protocol,
            config,
            request_id: 0,
            pending_outbound_handles: Default::default(),
            pending_events: Default::default(),
            connected_peers: Default::default(),
            addresses: Default::default(),
        }
    }

    pub fn send_request(
        &mut self,
        peer_id: &PeerId,
        request: C::Request,
        sender: OneshotSender<C::Response>,
    ) {
        let request_id = OutboundRequestId(self.request_id);
        self.request_id += 1;

        let handle = OutboundHandle::Request(request_id, request, sender);
        self.send(peer_id, handle, None);
    }

    pub fn send_response(
        &mut self,
        peer_id: &PeerId,
        connection_id: ConnectionId,
        request_id: InboundRequestId,
        response: C::Response,
        sender: OneshotSender<()>,
    ) {
        let handle = OutboundHandle::Response(request_id, response, sender);
        self.send(peer_id, handle, Some(connection_id));
    }

    pub fn update_rtt(&mut self, peer_id: &PeerId, connection_id: ConnectionId, rtt: Duration) {
        let connections = self.connected_peers.get_mut(peer_id).unwrap();
        let connection = connections
            .iter_mut()
            .find(|c| c.id == connection_id)
            .expect("Expected connection to be established before updating RTT.");
        connection.rtt = rtt;
    }

    fn send(
        &mut self,
        peer_id: &PeerId,
        handle: OutboundHandle<C::Request, C::Response>,
        connection_id: Option<ConnectionId>,
    ) {
        let connection = if let Some(connection_id) = connection_id {
            self.get_connection(peer_id, connection_id)
        } else {
            self.get_min_connection(peer_id)
        };
        if let Some(connection) = connection {
            let connection_id = connection.id;
            info!("Try to send through connection {:?}", connection_id);
            self.pending_events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer_id,
                handler: NotifyHandler::One(connection_id),
                event: handle,
            });
        } else {
            info!("Dialing {:?}", peer_id);
            self.pending_events.push_back(ToSwarm::Dial {
                opts: DialOpts::peer_id(*peer_id).build(),
            });
            self.pending_outbound_handles
                .entry(*peer_id)
                .or_default()
                .push(handle);
        }
    }

    fn get_connection(
        &mut self,
        peer_id: &PeerId,
        connection_id: ConnectionId,
    ) -> Option<&mut Connection> {
        self.connected_peers
            .get_mut(peer_id)
            .and_then(|connections| connections.iter_mut().find(|c| c.id == connection_id))
    }

    fn get_min_connection(&mut self, peer_id: &PeerId) -> Option<&mut Connection> {
        self.connected_peers
            .get_mut(peer_id)
            .and_then(|connections| connections.iter_mut().min_by_key(|c| c.rtt))
    }

    // fn try_send(
    //     &mut self,
    //     peer_id: &PeerId,
    //     message: OutboundMessage<C::Request, C::Response>,
    // ) -> Option<OutboundMessage<C::Request, C::Response>> {
    //     if let Some(connections) = self.connected_peers.get_mut(peer_id) {
    //         if let Some(connection) = connections.iter_mut().min_by_key(|c| c.rtt) {
    //             match message {
    //                 OutboundMessage::Request(request_id, _) => {
    //                     connection.pending_outbound_responses.insert(request_id);
    //                 }
    //                 OutboundMessage::Response(request_id, _) => {
    //                     connection.pending_inbound_responses.remove(&request_id);
    //                 }
    //             }
    //             self.pending_events.push_back(ToSwarm::NotifyHandler {
    //                 peer_id: *peer_id,
    //                 handler: NotifyHandler::One(connection.id),
    //                 event: message,
    //             });
    //             return None;
    //         }
    //     }

    //     Some(message)
    // }

    fn on_connection_closed(&mut self, ConnectionClosed { peer_id, .. }: ConnectionClosed) {
        info!("Connection closed: {:?}", peer_id);
        let connections = self
            .connected_peers
            .get_mut(&peer_id)
            .expect("Expected some established connection to peer before closing.");

        if connections.is_empty() {
            self.connected_peers.remove(&peer_id);
        }

        self.pending_events
            .push_back(ToSwarm::GenerateEvent(Event::Failure {
                peer_id,
                failure: ChannelFailure::ConnectionClosed,
            }));
    }

    fn on_dial_failure(&mut self, DialFailure { peer_id, error, .. }: DialFailure) {
        info!("Dial failure: {:?}", error);
        if let Some(peer_id) = peer_id {
            self.pending_events
                .push_back(ToSwarm::GenerateEvent(Event::Failure {
                    peer_id,
                    failure: ChannelFailure::DialFailure,
                }));
            if let Some(pending_handles) = self.pending_outbound_handles.remove(&peer_id) {
                for handle in pending_handles {
                    match handle {
                        OutboundHandle::Request(_, _, callback) => callback
                            .send(Err(io::Error::new(
                                io::ErrorKind::ConnectionRefused,
                                "Dial failure",
                            )))
                            .unwrap(),
                        OutboundHandle::Response(_, _, callback) => callback
                            .send(Err(io::Error::new(
                                io::ErrorKind::ConnectionRefused,
                                "Dial failure",
                            )))
                            .unwrap(),
                    }
                }
            }
        }
    }
}

impl<C> NetworkBehaviour for Behaviour<C>
where
    C: Codec + Send + Debug + Clone + Unpin + 'static,
{
    type ConnectionHandler = Handler<C>;

    type ToSwarm = Event<C::Request, C::Response>;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<libp2p_swarm::THandler<Self>, libp2p_swarm::ConnectionDenied> {
        info!("Established inbound connection: {:?}", peer_id);
        let handler = Handler::new(
            peer_id,
            connection_id,
            self.codec.clone(),
            self.protocol.clone(),
            self.config.request_timeout,
        );

        let connection = Connection::new(connection_id);
        self.connected_peers
            .entry(peer_id)
            .or_default()
            .push(connection);

        Ok(handler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        addr: &Multiaddr,
        _role_override: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        info!("Established outbound connection: {:?}", peer_id);
        info!(
            "Established outbound connection {:?} to {:?} at {:?}",
            connection_id, peer_id, addr
        );
        let mut handler = Handler::new(
            peer_id,
            connection_id,
            self.codec.clone(),
            self.protocol.clone(),
            self.config.request_timeout,
        );

        let connection = Connection::new(connection_id);

        if let Some(pending_handles) = self.pending_outbound_handles.remove(&peer_id) {
            for handle in pending_handles {
                handler.on_behaviour_event(handle);
            }
        }

        self.connected_peers
            .entry(peer_id)
            .or_default()
            .push(connection);

        Ok(handler)
    }

    fn on_swarm_event(&mut self, event: libp2p_swarm::FromSwarm) {
        self.addresses.on_swarm_event(&event);
        match event {
            FromSwarm::ConnectionEstablished(_) => {}
            FromSwarm::ConnectionClosed(connection_closed) => {
                self.on_connection_closed(connection_closed)
            }
            FromSwarm::DialFailure(dial_failure) => self.on_dial_failure(dial_failure),
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            handler::Event::Request {
                peer_id,
                connection_id,
                request_id,
                request,
            } => {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Request {
                        peer_id,
                        connection_id,
                        request_id,
                        request,
                    }));
            }
            handler::Event::MissedResponse {
                request_id,
                response,
            } => {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::MissedResponse {
                        request_id,
                        response,
                    }));
            }
            handler::Event::InboundStreamFailure(failure) => {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Failure { peer_id, failure }));
            }
            handler::Event::OutboundStreamFailure(failure) => {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Failure { peer_id, failure }));
            }
        };
    }

    fn poll(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.pending_events.pop_front() {
            Poll::Ready(event)
        } else {
            Poll::Pending
        }
    }
}

struct Connection {
    id: ConnectionId,
    rtt: Duration,
}

impl Connection {
    fn new(id: ConnectionId) -> Self {
        Self {
            id,
            rtt: Default::default(),
        }
    }
}

/// The events emitted by a request-response [`Behaviour`].
#[derive(Debug)]
pub enum Event<Req, Resp> {
    Request {
        peer_id: PeerId,
        connection_id: ConnectionId,
        request_id: InboundRequestId,
        request: Req,
    },
    MissedResponse {
        request_id: OutboundRequestId,
        response: Resp,
    },
    Failure {
        peer_id: PeerId,
        failure: ChannelFailure,
    },
}
