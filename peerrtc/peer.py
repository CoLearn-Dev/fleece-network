from abc import ABC, abstractmethod
import inspect
import anyio
from anyio import Event, create_memory_object_stream, to_thread
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
import logging
import pickle
from typing import Any, Callable, Coroutine, Generic, Optional, TypeVar
from aiortc import (  # type: ignore
    RTCPeerConnection,
    RTCConfiguration,
    RTCIceServer,
    RTCDataChannel,
    RTCSessionDescription,
)
from pydantic import BaseModel, validate_call
import websockets

from peerrtc.messages import (
    ConnectReply,
    ConnectRequest,
    SimpleReply,
    SimpleRequest,
    RegisterRequest,
)

logger = logging.getLogger(__name__)

P = TypeVar("P", bound=BaseModel)
R = TypeVar("R", bound=BaseModel)


class Outward(ABC, Generic[P, R]):
    @abstractmethod
    def label(self) -> str:
        pass

    @abstractmethod
    async def recv(self) -> tuple[str, R]:
        pass

    @abstractmethod
    async def send(self, op: str, data: P):
        pass

    @abstractmethod
    def close(self):
        pass


class Inward(ABC, Generic[P, R]):
    def __init__(
        self,
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
    ):
        self.hooks = hooks

    @abstractmethod
    def label(self) -> str:
        pass

    @abstractmethod
    async def _send(self, status: str, result: R):
        pass

    async def handle(self, op: str, data: P):
        callback = self.hooks.get(op)

        async def ahandler(callback: Callable[[P], Coroutine[Any, Any, tuple[str, R]]]):
            status, result = await callback(data)
            if result != None:
                await self._send(status, result)

        async def handler(callback: Callable[[P], tuple[str, R]]):
            status, result = await to_thread.run_sync(callback, data)
            if result != None:
                await self._send(status, result)

        if callback is not None:
            if inspect.iscoroutinefunction(callback):
                await ahandler(callback)
            elif inspect.isfunction(callback):
                await handler(callback)
            else:
                raise ValueError("Invalid callback type")


class Connection(ABC):
    @abstractmethod
    async def send(self, op: str, data: P):
        pass

    @abstractmethod
    async def recv(self) -> Optional[tuple[str, Any]]:
        pass


class OutwardDataChannel(Outward):
    def __init__(self, channel: RTCDataChannel):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.channel = channel
        self.send_stream, self.recv_stream = create_memory_object_stream[
            tuple[str, Any]
        ]()
        self.isopen = Event()

        @channel.on("open")
        def on_open():
            self.isopen.set()
            self._logger.info("Outward data channel %s opens", self.label())

        @channel.on("message")
        async def on_message(raw: bytes):
            self._logger.info(
                "Outward data channel %s receives message: %s", self.label(), raw
            )
            reply: SimpleReply = pickle.loads(raw)
            await self.send_stream.send((reply.status, reply.data))

        @channel.on("close")
        async def on_close():
            self._logger.warning("Outward data channel %s closes", self.label())

    def label(self) -> str:
        return self.channel.label

    async def recv(self) -> tuple[str, R]:
        return await self.recv_stream.receive()

    async def send(self, op: str, data: P):
        await self.isopen.wait()
        self.channel.send(pickle.dumps(SimpleRequest(op, data)))
        self._logger.info(
            "Outward data channel %s sends message: %s %s", self.label(), op, data
        )

    def close(self):
        self.channel.close()


class InwardDataChannel(Inward):
    def __init__(
        self,
        channel: RTCDataChannel,
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
    ):
        super().__init__(hooks)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.channel = channel

        @channel.on("open")
        async def on_open():
            self._logger.info("Inward data channel opened: %s", self.label())

        @channel.on("message")
        async def on_message(raw: bytes):
            message: SimpleRequest = pickle.loads(raw)
            await self.handle(message.op, message.data)

        @channel.on("close")
        async def on_close():
            self._logger.warning("Inward data channel %s closes", self.label())

    def label(self) -> str:
        return self.channel.label

    async def _send(self, status: str, result: P):
        """Although it's not an async function, it requires the existence of an event loop."""

        reply = SimpleReply(status, result)
        self._logger.info("Channel %s sends message: %s", self.label(), reply)
        self.channel.send(pickle.dumps(reply))


class OutwardLoopback(Outward, Generic[P, R]):
    def __init__(
        self,
        label: str,
        send_stream: MemoryObjectSendStream[tuple[str, P]],
        recv_stream: MemoryObjectReceiveStream[tuple[str, R]],
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._label = label

        self.send_stream = send_stream
        """Used to send request to the other side"""

        self.recv_stream = recv_stream
        """Used to receive reply to the other side"""

    def label(self) -> str:
        return self._label

    async def recv(self) -> tuple[str, R]:
        return await self.recv_stream.receive()

    async def send(self, op: str, data: P):
        await self.send_stream.send((op, data))

    def close(self):
        self.send_stream.close()
        self.recv_stream.close()


class InwardLoopback(Inward, Generic[P, R]):
    def __init__(
        self,
        label: str,
        recv_stream: MemoryObjectReceiveStream[tuple[str, P]],
        send_stream: MemoryObjectSendStream[tuple[str, R]],
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
        tg: TaskGroup,
    ):
        super().__init__(hooks)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._label = label
        self.send_stream = send_stream
        self.recv_stream = recv_stream

        async def onmessage():
            async for message in recv_stream:
                await self.handle(message[0], message[1])

        tg.start_soon(onmessage)

    def label(self) -> str:
        return self._label

    async def _send(self, status: str, result: R):
        """Although it's not an async function, it requires the existence of an event loop."""

        await self.send_stream.send((status, result))
        self._logger.info(
            "Channel %s sends message: %s", self.label(), (status, result)
        )


class PeerConnection(Connection):
    class State:
        DEAD = 0
        OFFERED = 1
        WAITING = 2
        CONNECTED = 3

    def __init__(
        self,
        from_id: str,
        to_id: str,
        configs: list[tuple[str, Optional[str], Optional[str]]],
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.from_id = from_id
        self.to_id = to_id
        self.configs = configs
        self.inner: Optional[RTCPeerConnection] = None  # changed with state
        self.hooks = hooks

        self.state = PeerConnection.State.DEAD
        self.in_channel: Optional[InwardDataChannel] = None
        self.out_channel: Optional[OutwardDataChannel] = None
        self.recv_stream = anyio.Lock()
        self.lock = anyio.Lock()
        self.condition = anyio.Condition(self.lock)

    async def _kill_inner(self):
        self.state = PeerConnection.State.DEAD
        if self.inner is not None:
            await self.inner.close()
            self.inner = None
        self.in_channel = None
        self.out_channel = None

    async def _init_inner(self) -> RTCPeerConnection:
        pc = RTCPeerConnection(
            RTCConfiguration(
                [
                    RTCIceServer(config[0], config[1], config[2])
                    for config in self.configs
                ]
            )
        )
        self.out_channel = OutwardDataChannel(pc.createDataChannel(self.from_id))
        self.inner = pc

        @pc.on("datachannel")
        async def on_datachannel(channel: RTCDataChannel):
            async with self.lock:
                self.in_channel = InwardDataChannel(channel, self.hooks)
                self._logger.info("Data channel created: %s", self.in_channel.label())

        @pc.on("connectionstatechange")
        async def on_connectionstatechange():
            async with self.condition:
                if pc.connectionState == "connected":
                    self.state = PeerConnection.State.CONNECTED
                    self.condition.notify_all()

                    self._logger.info(
                        "Connection (%s, %s) changes state to CONNECTED",
                        self.from_id,
                        self.to_id,
                    )
                elif pc.connectionState == "failed":
                    await self._kill_inner()

                    self._logger.info(
                        "Connection (%s, %s) changes state to FAILED",
                        self.from_id,
                        self.to_id,
                    )

        return pc

    async def create_offer(self) -> Optional[RTCSessionDescription]:
        """For return part, None means connected."""
        async with self.lock:
            if self.state != PeerConnection.State.DEAD:
                self._logger.info("No need to create offer")
                return None

            self.state = PeerConnection.State.OFFERED
            pc = await self._init_inner()
            await pc.setLocalDescription(await pc.createOffer())

            self._logger.info("Create offer with %s", pc.localDescription)

            return pc.localDescription

    async def create_answer(
        self, sdp: RTCSessionDescription
    ) -> Optional[RTCSessionDescription]:
        async with self.condition:
            if sdp.type != "offer":
                self._logger.warning(
                    "Invalid sdp type: %s for creating answer", sdp.type
                )
                return None

            if self.state == PeerConnection.State.CONNECTED:
                # the peer might lose connection and try to reconnect
                await self._kill_inner()
                self.condition.notify_all()
                self._logger.info("Reconnected and create answer")

            # when both peers want to establish connection, allow the one with smaller id to be the offerer
            if self.state == PeerConnection.State.OFFERED:
                if self.from_id < self.to_id:
                    self._logger.info(
                        "Both peer want to establish connection, but I'm the offerer."
                    )
                    return None
                else:
                    await self._kill_inner()
                    self.condition.notify_all()
                    self._logger.info(
                        "Both peer want to establish connection, but I'm the answerer."
                    )

            self.state = PeerConnection.State.WAITING
            pc = await self._init_inner()
            await pc.setRemoteDescription(sdp)
            await pc.setLocalDescription(await pc.createAnswer())

            return pc.localDescription

    async def set_answer(self, sdp: Optional[RTCSessionDescription]):
        async with self.lock:
            if sdp is None:
                if self.state == PeerConnection.State.OFFERED:
                    await self._kill_inner()  # the remote refused to give an answer

                    self._logger.warning("The remote refused to give an answer")
            else:
                if sdp.type != "answer":
                    self._logger.warning(
                        "Invalid sdp type: %s for setting answer", sdp.type
                    )
                    return None

                if self.state != PeerConnection.State.OFFERED:
                    return None

                self.state = PeerConnection.State.WAITING
                pc = self.inner
                if pc is not None:
                    await pc.setRemoteDescription(sdp)
                else:
                    self._logger.error("No inner peer connection")

    async def send(
        self,
        op: str,
        data: P,
    ):
        while True:
            async with self.condition:
                if self.state == PeerConnection.State.CONNECTED:
                    if self.out_channel is not None:
                        self._logger.info("Sending message: %s", op)
                        await self.out_channel.send(op, data)
                        return
                    else:
                        self._logger.error(
                            "No outward data channel within connected connection"
                        )
                elif self.state == PeerConnection.State.DEAD:
                    raise Exception("Connection is dead")
                else:
                    await self.condition.wait()

    async def recv(self) -> Optional[tuple[str, Any]]:
        if self.out_channel is None:
            return None
        return await self.out_channel.recv()


class SelfConnection(Connection):
    def __init__(
        self,
        id: str,
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
        tg: TaskGroup,
    ):
        req_send, req_recv = create_memory_object_stream[tuple[str, P]]()
        rep_send, rep_recv = create_memory_object_stream[tuple[str, R]]()
        self.out_loop = OutwardLoopback(id, req_send, rep_recv)
        self.in_loop = InwardLoopback(id, req_recv, rep_send, hooks, tg)

    async def send(self, op: str, data: P):
        await self.out_loop.send(op, data)

    async def recv(self) -> Optional[tuple[str, Any]]:
        return await self.out_loop.recv()


class Peer:
    def __init__(
        self,
        worker_id: str,
        signaling_url: str,
        ice_configs: list[tuple[str, Optional[str], Optional[str]]],
        hooks: dict[
            str,
            Callable[[P], Coroutine[Any, Any, tuple[str, R]]]
            | Callable[[P], tuple[str, R]],
        ],
        tg: TaskGroup,
    ):

        self._logger = logging.getLogger(self.__class__.__name__)
        self.worker_id = worker_id
        """A unique id for worker. Only for identification."""

        self.signaling_url = signaling_url
        """The signaling server url should not contain the protocol."""

        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        """The websocket connection to the signaling server. Use to send offer."""

        self.hooks = {name: validate_call(hook) for name, hook in hooks.items()}
        """Every time a new channel is created by peer (not by our), this handler will be called."""

        self.tg = tg

        self.ice_configs = ice_configs

        self.conns: dict[str, PeerConnection] = {}
        self.lo = SelfConnection(worker_id, self.hooks, tg)
        self.lock = anyio.Lock()

        self.tg.start_soon(self._register)

    async def _answer(
        self, ws: websockets.WebSocketClientProtocol, request: ConnectRequest
    ):
        from_worker_id = request.from_worker_id
        self._logger.info("Received offer from %s", from_worker_id)

        async with self.lock:
            if from_worker_id not in self.conns:
                self.conns[from_worker_id] = PeerConnection(
                    self.worker_id,
                    from_worker_id,
                    self.ice_configs,
                    self.hooks,
                )
            pc = self.conns[from_worker_id]
            answer = await pc.create_answer(request.sdp)
            answermsg = ConnectReply(self.worker_id, from_worker_id, answer)
            await ws.send(pickle.dumps(answermsg))

    async def _resolve(self, reply: ConnectReply):
        async with self.lock:
            await self.conns[reply.from_worker_id].set_answer(reply.sdp)

    async def _register(self):
        while True:
            try:
                async with websockets.connect(f"{self.signaling_url}/register") as ws:
                    # send register message first
                    await ws.send(pickle.dumps(RegisterRequest(self.worker_id)))
                    self._logger.info("Registering worker: %s", self.worker_id)

                    # chec whether accepted
                    reply: RegisterReply = pickle.loads(await ws.recv())  # type: ignore
                    if reply.status != "ok":
                        break

                    async with self.lock:
                        self.ws = ws
                    self._logger.info("Connected to signaling server")

                    while True:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            message = pickle.loads(raw)

                            # TODO: determine whether use worker thread
                            if isinstance(message, ConnectRequest):
                                self.tg.start_soon(self._answer, ws, message)
                            elif isinstance(message, ConnectReply):
                                self.tg.start_soon(self._resolve, message)

                                pass
                            else:
                                self._logger.error(
                                    "Unknown message type %s",
                                    message.__class__.__name__,
                                )
            except Exception as e:
                self._logger.warn("Failed to connect to signaling server due to %s", e)
                await anyio.sleep(1)

    async def connect(self, to_worker_id: str) -> Optional[Connection]:
        if to_worker_id == self.worker_id:
            return self.lo

        async with self.lock:
            if to_worker_id not in self.conns:
                self.conns[to_worker_id] = PeerConnection(
                    self.worker_id, to_worker_id, self.ice_configs, self.hooks
                )
            pc = self.conns[to_worker_id]
            ws = self.ws
            if ws is None:
                return None  # not connected to signaling server

        offer = await pc.create_offer()
        if offer is not None:
            if ws is not None:
                await ws.send(
                    pickle.dumps(ConnectRequest(self.worker_id, to_worker_id, offer))
                )
            return pc
        else:
            return None
