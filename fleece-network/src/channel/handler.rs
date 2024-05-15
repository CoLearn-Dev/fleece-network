use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    io, mem,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{stream::FuturesUnordered, Future, FutureExt, SinkExt, StreamExt};
use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::PeerId;
use libp2p_swarm::{
    handler::{
        ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
        ListenUpgradeError,
    },
    ConnectionId,
};
use libp2p_swarm::{
    ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
};
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tracing::info;

use crate::channel::InboundHandle;

use super::{
    codec::{Codec, CodecSink, CodecStream},
    message::{InboundMessage, InboundRequestId, OutboundRequestId},
    OneshotSender, OutboundHandle, OutboundMessage,
};

pub struct Handler<C: Codec> {
    peer_id: PeerId,
    connection_id: ConnectionId,
    codec: C,

    stream: StatedStream<CodecStream<C::Decoder>>,
    sink: StatedSink<CodecSink<C::Encoder>>,

    request_timeout: Duration,

    protocol: C::Protocol,
    pending_events: VecDeque<Event<C>>,
    pending_outbound_handles: VecDeque<OutboundHandle<C::Request, C::Response>>,
    pending_outbound_response: VecDeque<OutboundMessage<C::Request, C::Response>>,
    inbound_request_sender: mpsc::Sender<InboundHandle<C::Request, C::Response>>,
    outbound_request_callbacks: HashMap<OutboundRequestId, OneshotSender<C::Response>>,
    inbound_request_futures:
        FuturesUnordered<Pin<Box<dyn Future<Output = (InboundRequestId, C::Response)> + Send>>>,
    timeout_futures: FuturesUnordered<Pin<Box<dyn Future<Output = OutboundRequestId> + Send>>>,
    workers: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl<C: Codec> Handler<C> {
    pub(super) fn new(
        peer_id: PeerId,
        connection_id: ConnectionId,
        codec: C,
        protocol: C::Protocol,
        inbound_request_sender: mpsc::Sender<InboundHandle<C::Request, C::Response>>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            peer_id,
            connection_id,
            codec,
            stream: StatedStream::Pending(None),
            sink: StatedSink::Idle,
            request_timeout,
            protocol,
            pending_events: Default::default(),
            pending_outbound_handles: Default::default(),
            pending_outbound_response: Default::default(),
            inbound_request_sender,
            outbound_request_callbacks: Default::default(),
            inbound_request_futures: Default::default(),
            timeout_futures: Default::default(),
            workers: Default::default(),
        }
    }
}

impl<C> Handler<C>
where
    C: Codec + Send + Debug + Clone + Unpin + 'static,
{
    fn send(&mut self, handle: OutboundHandle<C::Request, C::Response>) {
        match &mut self.sink {
            StatedSink::Active(sink, waker) => {
                let request_id = handle.id;
                let (message, callback) = handle.split();
                if let Err(_) = sink.start_send_unpin(message) {
                    self.sink = StatedSink::Failed(None);
                    callback
                        .send(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "Buffer is full",
                        )))
                        .unwrap();
                } else {
                    info!("Insert callback for request {:?}", request_id);
                    self.outbound_request_callbacks.insert(request_id, callback);
                    let request_timeout = self.request_timeout.clone();
                    self.timeout_futures.push(
                        async move {
                            time::sleep(request_timeout).await;
                            request_id
                        }
                        .boxed(),
                    );
                    waker.as_ref().map(|waker| waker.wake_by_ref());
                }
            }

            StatedSink::Failed(waker) => {
                waker.as_ref().map(|waker| waker.wake_by_ref());
                self.sink = StatedSink::Idle;
                self.pending_outbound_handles.push_back(handle);
            }

            _ => {
                self.pending_outbound_handles.push_back(handle);
            }
        }
    }

    fn flush(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::ToBehaviour,
        >,
    > {
        match &mut self.sink {
            StatedSink::Idle => {
                self.sink = StatedSink::Pending(None);
                info!("Send outbound substream request to {:?}", self.peer_id);
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                    protocol: SubstreamProtocol::new(ReadyUpgrade::new(self.protocol.clone()), ()),
                });
            }
            StatedSink::Active(sink, waker) => {
                waker.replace(cx.waker().clone());
                match sink.poll_flush_unpin(cx) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {}
                        Err(_) => {
                            info!("Fail when write into sink");
                            self.sink = StatedSink::Failed(Some(cx.waker().clone()));
                            let request_callbacks = mem::replace(
                                &mut self.outbound_request_callbacks,
                                Default::default(),
                            );
                            request_callbacks.into_values().for_each(|callback| {
                                callback
                                    .send(Err(io::Error::new(
                                        io::ErrorKind::BrokenPipe,
                                        "Failed to send",
                                    )))
                                    .unwrap();
                            });
                        }
                    },
                    Poll::Pending => {}
                }
            }
            StatedSink::Failed(_) => {
                self.sink = StatedSink::Failed(Some(cx.waker().clone()));
            }
            StatedSink::Pending(_) => {
                self.sink = StatedSink::Pending(Some(cx.waker().clone()));
            }
        }

        Poll::Pending
    }

    fn recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::ToBehaviour,
        >,
    > {
        loop {
            match &mut self.stream {
                StatedStream::Active(stream) => match stream.poll_next_unpin(cx) {
                    Poll::Ready(option) => match option {
                        Some(message) => match message {
                            InboundMessage::Request(request_id, request) => {
                                info!("Received request from {:?}", self.peer_id);
                                let (sender, receiver) = oneshot::channel();
                                self.inbound_request_futures.push(Box::pin(async move {
                                    (request_id, receiver.await.unwrap())
                                }));
                                let handle = InboundHandle::new(request_id, request, sender);
                                let sender = self.inbound_request_sender.clone();
                                self.workers.push(Box::pin(async move {
                                    sender.send(handle).await.unwrap();
                                }));
                            }
                            InboundMessage::Response(request_id, response) => {
                                info!(
                                    "Received response from {:?} for {:?}",
                                    self.peer_id, request_id
                                );
                                if let Some(sender) =
                                    self.outbound_request_callbacks.remove(&request_id)
                                {
                                    sender.send(Ok(response)).unwrap();
                                } else {
                                    return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                        Event::MissedResponse {
                                            request_id,
                                            response,
                                        },
                                    ));
                                }
                            }
                        },
                        None => {
                            info!("Inbound stream {:?} fails", self.peer_id);
                            self.stream = StatedStream::Pending(Some(cx.waker().clone()));
                            break;
                        }
                    },
                    Poll::Pending => break,
                },

                StatedStream::Pending(_) => {
                    self.stream = StatedStream::Pending(Some(cx.waker().clone()));
                    break;
                }
            }
        }

        Poll::Pending
    }

    fn reply(&mut self, message: OutboundMessage<C::Request, C::Response>) {
        match &mut self.sink {
            StatedSink::Active(sink, waker) => {
                if let Err(_) = sink.start_send_unpin(message) {
                    self.sink = StatedSink::Failed(None);
                } else {
                    waker.as_ref().map(|waker| waker.wake_by_ref());
                }
            }

            _ => {
                self.pending_outbound_response.push_back(message);
            }
        }
    }

    fn wakeup_stream(&mut self) {
        match &self.stream {
            StatedStream::Pending(Some(waker)) => waker.wake_by_ref(),
            _ => {}
        }
    }

    fn wakeup_sink(&mut self) {
        match &self.sink {
            StatedSink::Pending(Some(waker)) => waker.wake_by_ref(),
            StatedSink::Failed(Some(waker)) => waker.wake_by_ref(),
            _ => {}
        }
    }

    fn on_fully_negotiated_inbound(
        &mut self,
        FullyNegotiatedInbound {
            protocol: stream,
            info: (),
        }: FullyNegotiatedInbound<
            <Self as ConnectionHandler>::InboundProtocol,
            <Self as ConnectionHandler>::InboundOpenInfo,
        >,
    ) {
        self.wakeup_stream();
        self.stream = StatedStream::Active(CodecStream::new(stream, self.codec.new_decoder()));
    }

    fn on_fully_negotiated_outbound(
        &mut self,
        FullyNegotiatedOutbound {
            protocol: stream,
            info: (),
        }: FullyNegotiatedOutbound<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
        >,
    ) {
        self.wakeup_sink();
        self.sink = StatedSink::Active(CodecSink::new(stream, self.codec.new_encoder()), None);

        let pending_outbound_handles =
            mem::replace(&mut self.pending_outbound_handles, VecDeque::new());
        for message in pending_outbound_handles {
            self.send(message);
        }

        let pending_outbound_messages =
            mem::replace(&mut self.pending_outbound_response, VecDeque::new());
        for message in pending_outbound_messages {
            self.reply(message);
        }
    }

    fn on_dial_upgrade_error(
        &mut self,
        DialUpgradeError { error, info: () }: DialUpgradeError<
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::OutboundProtocol,
        >,
    ) {
        info!("Dial upgrade error: {:?}", error);
        match error {
            StreamUpgradeError::Timeout => {
                self.pending_events
                    .push_back(Event::OutboundStreamFailure(ChannelFailure::Timeout));
            }
            StreamUpgradeError::NegotiationFailed => {
                self.pending_events.push_back(Event::OutboundStreamFailure(
                    ChannelFailure::UnsupportedProtocols,
                ));
            }
            StreamUpgradeError::Apply(e) => void::unreachable(e),
            StreamUpgradeError::Io(e) => {
                todo!("Handle I/O error: {:?}", e);
            }
        }
    }

    fn on_listen_upgrade_error(
        &mut self,
        ListenUpgradeError { error, .. }: ListenUpgradeError<
            <Self as ConnectionHandler>::InboundOpenInfo,
            <Self as ConnectionHandler>::InboundProtocol,
        >,
    ) {
        info!("Listen upgrade error: {:?}", error);
        void::unreachable(error)
    }
}

impl<C> ConnectionHandler for Handler<C>
where
    C: Codec + Send + Debug + Clone + Unpin + 'static,
{
    type FromBehaviour = OutboundHandle<C::Request, C::Response>;

    type ToBehaviour = Event<C>;

    type InboundProtocol = ReadyUpgrade<C::Protocol>;

    type OutboundProtocol = ReadyUpgrade<C::Protocol>;

    type InboundOpenInfo = ();

    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(self.protocol.clone()), ())
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        if let Poll::Ready(event) = self.flush(cx) {
            return Poll::Ready(event);
        }

        if let Poll::Ready(event) = self.recv(cx) {
            return Poll::Ready(event);
        }

        while !self.inbound_request_futures.is_empty() {
            match self.inbound_request_futures.poll_next_unpin(cx) {
                Poll::Ready(Some((request_id, response))) => {
                    self.reply(OutboundMessage::Response(request_id, response));
                }
                _ => break,
            }
        }

        while !self.timeout_futures.is_empty() {
            match self.timeout_futures.poll_next_unpin(cx) {
                Poll::Ready(Some(request_id)) => {
                    if let Some(sender) = self.outbound_request_callbacks.remove(&request_id) {
                        sender
                            .send(Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "Request timed out",
                            )))
                            .unwrap();
                    }
                }
                _ => break,
            }
        }

        while !self.workers.is_empty() {
            match self.workers.poll_next_unpin(cx) {
                Poll::Ready(Some(_)) => {}
                _ => break,
            }
        }

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: OutboundHandle<C::Request, C::Response>) {
        self.send(event);
    }

    fn on_connection_event(
        &mut self,
        event: libp2p_swarm::handler::ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        info!("Channel {:?} on event: {:?}", self.peer_id, event);
        match event {
            ConnectionEvent::FullyNegotiatedInbound(fully_negotiated_inbound) => {
                self.on_fully_negotiated_inbound(fully_negotiated_inbound)
            }
            ConnectionEvent::FullyNegotiatedOutbound(fully_negotiated_outbound) => {
                self.on_fully_negotiated_outbound(fully_negotiated_outbound)
            }
            ConnectionEvent::DialUpgradeError(dial_upgrade_error) => {
                self.on_dial_upgrade_error(dial_upgrade_error)
            }
            ConnectionEvent::ListenUpgradeError(listen_upgrade_error) => {
                self.on_listen_upgrade_error(listen_upgrade_error)
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub enum StatedStream<S> {
    Pending(Option<Waker>),
    Active(S),
}

#[derive(Debug)]
pub enum StatedSink<S> {
    Idle,
    Pending(Option<Waker>),
    Failed(Option<Waker>),
    Active(S, Option<Waker>),
}

#[derive(Debug)]
pub enum Event<C>
where
    C: Codec,
{
    MissedResponse {
        request_id: OutboundRequestId,
        response: C::Response,
    },
    InboundStreamFailure(ChannelFailure),
    OutboundStreamFailure(ChannelFailure),
}

#[derive(Debug)]
pub enum ChannelFailure {
    DialFailure,
    Timeout,
    ConnectionClosed,
    UnsupportedProtocols,
    Io(io::Error),
}
