from abc import ABC, abstractmethod
import asyncio
import functools
import inspect
import anyio
from anyio import Event, create_memory_object_stream, to_thread
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
import logging
import pickle
from typing import (
    Any,
    Callable,
    Coroutine,
    Optional,
    Protocol,
    TypeVar,
    Union,
    get_type_hints,
)
from .aiortc import (  # type: ignore
    RTCPeerConnection,
    RTCConfiguration,
    RTCIceServer,
    RTCDataChannel,
    RTCSessionDescription,
)
from fastapi import HTTPException, Response
from pydantic import BaseModel
import websockets

from .messages import (
    ConnectReply,
    ConnectRequest,
    SimpleReply,
    SimpleRequest,
    RegisterRequest,
)

logger = logging.getLogger(__name__)


class Encodable(Protocol):
    @abstractmethod
    def encode(self, charset: str) -> bytes: ...


P = TypeVar("P", bound=BaseModel)
E = TypeVar("E", bound=Encodable)
R = Union[E, str]
SyncHandler = Callable[[P], R]
AsyncHandler = Callable[[P], Coroutine[Any, Any, R]]


class Outward(ABC):
    @abstractmethod
    def label(self) -> str:
        pass

    @abstractmethod
    async def send(self, op: str, data: P) -> Response:
        pass

    @abstractmethod
    def close(self):
        pass


class Inward(ABC):
    def __init__(
        self,
        hooks: dict[str, SyncHandler | AsyncHandler],
    ):
        self.hooks = hooks

    @abstractmethod
    def label(self) -> str:
        pass

    @abstractmethod
    async def _send(self, id: int, reply: Response):
        pass

    async def handle(self, id: int, op: str, data: P):
        callback = self.hooks.get(op)

        async def ahandler(callback: AsyncHandler):
            return await callback(data)

        async def handler(callback: SyncHandler):
            return await to_thread.run_sync(callback, data)

        if callback is not None:
            if not inspect.isfunction(callback):  # assertion
                raise ValueError("Invalid callback type")
            try:
                if inspect.iscoroutinefunction(callback):
                    result = await ahandler(callback)
                else:
                    result = await handler(callback)
            except HTTPException as e:
                await self._send(id, Response(e.detail, e.status_code, e.headers))
            else:
                if isinstance(result, BaseModel):
                    await self._send(id, Response(result.model_dump_json()))
                elif isinstance(result, str):
                    await self._send(id, Response(result))
                elif result is None:
                    await self._send(id, Response())
                else:  # assertion
                    raise ValueError("Invalid result type")


class Connection(ABC):
    @abstractmethod
    async def send(self, op: str, data: P) -> Response:
        pass


class OutwardDataChannel(Outward):
    def __init__(self, channel: RTCDataChannel):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.channel = channel
        self.isopen = Event()
        self.lock = anyio.Lock()
        self.map: dict[int, MemoryObjectSendStream[Response]] = {}
        self.counter = 0

        def on_open():
            self.isopen.set()
            self._logger.info("Outward data channel %s opens", self.label())

        async def on_message(raw: bytes):
            self._logger.info("Outward data channel %s receives message", self.label())
            reply: SimpleReply = pickle.loads(raw)
            async with self.lock:
                send_stream = self.map.pop(reply.id)
                await send_stream.send(reply.data)

        async def on_close():
            self._logger.warning("Outward data channel %s closes", self.label())

        channel.on("open", on_open)
        channel.on("message", on_message)
        channel.on("close", on_close)

    def label(self) -> str:
        return self.channel.label

    async def send(self, op: str, data: P) -> Response:
        await self.isopen.wait()
        send_stream, recv_stream = create_memory_object_stream[Response]()
        async with self.lock:
            id = self.counter
            self.counter += 1
            self.map[id] = send_stream
        self.channel.send(pickle.dumps(SimpleRequest(id, op, data)))
        self._logger.info("Outward data channel %s sends message: %s", self.label(), op)
        return await recv_stream.receive()

    def close(self):
        self.channel.close()


class InwardDataChannel(Inward):
    def __init__(
        self,
        channel: RTCDataChannel,
        hooks: dict[str, SyncHandler | AsyncHandler],
    ):
        super().__init__(hooks)
        self._logger = logging.getLogger(self.__class__.__name__)
        self.channel = channel

        async def on_open():
            self._logger.info("Inward data channel opened: %s", self.label())

        async def on_message(raw: bytes):
            message: SimpleRequest = pickle.loads(raw)
            await self.handle(message.id, message.op, message.data)

        async def on_close():
            self._logger.warning("Inward data channel %s closes", self.label())

        channel.on("open", on_open)
        channel.on("message", on_message)
        channel.on("close", on_close)

    def label(self) -> str:
        return self.channel.label

    async def _send(self, id: int, reply: Response):
        """Although it's not an async function, it requires the existence of an event loop."""

        self._logger.info("Inward data channel %s sends message", self.label())
        self.channel.send(pickle.dumps(SimpleReply(id, reply)))


class OutwardLoopback(Outward):
    def __init__(
        self,
        label: str,
        send_stream: MemoryObjectSendStream[tuple[int, str, P]],
        recv_stream: MemoryObjectReceiveStream[tuple[int, Response]],
        tg: TaskGroup,
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._label = label

        self.send_stream = send_stream
        """Used to send request to the other side"""

        self.recv_stream = recv_stream
        """Used to receive reply to the other side"""

        self.lock = anyio.Lock()
        self.map: dict[int, MemoryObjectSendStream[Response]] = {}
        self.counter = 0

        async def onmessage():
            async for message in recv_stream:
                id, reply = message
                async with self.lock:
                    send_stream = self.map.pop(id)
                tg.start_soon(send_stream.send, reply)
                self._logger.info(
                    "Outward data channel %s receives message", self.label()
                )

        tg.start_soon(onmessage)

    def label(self) -> str:
        return self._label

    async def send(self, op: str, data: P) -> Response:
        send_stream, recv_stream = create_memory_object_stream[Response]()
        async with self.lock:
            id = self.counter
            self.counter += 1
            self.map[id] = send_stream
        await self.send_stream.send((id, op, data))
        self._logger.info("Outward data channel %s sends message: %s", self.label(), op)
        return await recv_stream.receive()

    def close(self):
        self.send_stream.close()
        self.recv_stream.close()


class InwardLoopback(Inward):
    def __init__(
        self,
        label: str,
        recv_stream: MemoryObjectReceiveStream[tuple[int, str, P]],
        send_stream: MemoryObjectSendStream[tuple[int, Response]],
        hooks: dict[str, SyncHandler | AsyncHandler],
        tg: TaskGroup,
    ):
        super().__init__(hooks)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._label = label
        self.send_stream = send_stream
        self.recv_stream = recv_stream

        async def onmessage():
            async for message in recv_stream:
                tg.start_soon(self.handle, message[0], message[1], message[2])

        tg.start_soon(onmessage)

    def label(self) -> str:
        return self._label

    async def _send(self, id: int, reply: Response):
        """Although it's not an async function, it requires the existence of an event loop."""

        await self.send_stream.send((id, reply))
        self._logger.info("Inward data channel %s sends message", self.label())


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
        hooks: dict[str, AsyncHandler | SyncHandler],
        tg: TaskGroup,
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.from_id = from_id
        self.to_id = to_id
        self.configs = configs
        self.inner: Optional[RTCPeerConnection] = None  # changed with state
        self.hooks = hooks
        self.tg = tg

        self.state = PeerConnection.State.DEAD
        self.in_channel: Optional[InwardDataChannel] = None
        self.out_channel: Optional[OutwardDataChannel] = None
        self.recv_stream = anyio.Lock()
        self.lock = anyio.Lock()
        self.condition = anyio.Condition(self.lock)

    def _reset_inner(self):
        self.state = PeerConnection.State.DEAD
        self.in_channel = None
        self.out_channel = None

    async def _kill_inner(self):
        self._reset_inner()
        if self.inner is not None:
            self.tg.start_soon(self.inner.close)
            self.inner = None

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

        async def on_datachannel(channel: RTCDataChannel):
            async with self.lock:
                self.in_channel = InwardDataChannel(channel, self.hooks)
                self._logger.info("Data channel created: %s", self.in_channel.label())

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
                elif pc.connectionState == "failed" or pc.connectionState == "closed":
                    self._reset_inner()

                    self._logger.info(
                        "Connection (%s, %s) changes state to FAILED",
                        self.from_id,
                        self.to_id,
                    )

        pc.on("datachannel", on_datachannel)
        pc.on("connectionstatechange", on_connectionstatechange)

        return pc

    async def send_through_ws(
        self, ws: websockets.WebSocketClientProtocol, sdp: RTCSessionDescription
    ):
        try:
            if sdp.type == "offer":
                await ws.send(
                    pickle.dumps(ConnectRequest(self.from_id, self.to_id, sdp))
                )
            else:
                await ws.send(pickle.dumps(ConnectReply(self.from_id, self.to_id, sdp)))
        except:
            async with self.condition:
                await self._kill_inner()
            self._logger.error("Failed to send sdp")

    async def create_offer(
        self, ws: Optional[websockets.WebSocketClientProtocol]
    ) -> bool:
        if ws is None:
            return False

        """For return part, None means connected."""
        async with self.lock:
            if self.state != PeerConnection.State.DEAD:
                self._logger.info("No need to create offer")
                return False

            self.state = PeerConnection.State.OFFERED
            pc = await self._init_inner()
            await pc.setLocalDescription(await pc.createOffer())
            offer = pc.localDescription
            assert offer

            self._logger.info("Create offer with %s", offer)

        self.tg.start_soon(self.send_through_ws, ws, offer)

        return True

    async def create_answer(
        self, ws: websockets.WebSocketClientProtocol, sdp: RTCSessionDescription
    ) -> bool:
        async with self.condition:
            if sdp.type != "offer":
                self._logger.warning(
                    "Invalid sdp type: %s for creating answer", sdp.type
                )
                return False

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
                    return False
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
            answer = pc.localDescription
            assert answer

        self.tg.start_soon(self.send_through_ws, ws, answer)

        return True

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

    async def send(self, op: str, data: P) -> Response:
        while True:
            async with self.condition:
                if self.state == PeerConnection.State.CONNECTED:
                    if self.out_channel is not None:
                        self._logger.info("Sending message: %s", op)
                        channel = self.out_channel
                        break
                    else:
                        self._logger.error(
                            "No outward data channel within connected connection"
                        )
                elif self.state == PeerConnection.State.DEAD:
                    raise Exception("Connection is dead")  # TODO: better exception
                else:
                    await self.condition.wait()

        return await channel.send(op, data)


class SelfConnection(Connection):
    def __init__(
        self,
        id: str,
        hooks: dict[str, AsyncHandler | SyncHandler],
        tg: TaskGroup,
    ):
        req_send, req_recv = create_memory_object_stream[tuple[int, str, BaseModel]]()
        rep_send, rep_recv = create_memory_object_stream[tuple[int, Response]]()
        self.out_loop = OutwardLoopback(id, req_send, rep_recv, tg)
        self.in_loop = InwardLoopback(id, req_recv, rep_send, hooks, tg)

    async def send(self, op: str, data: P) -> Response:
        return await self.out_loop.send(op, data)


class Peer:
    def __init__(
        self,
        worker_id: str,
        signaling_url: str,
        ice_configs: list[tuple[str, Optional[str], Optional[str]]],
        hooks: dict[str, AsyncHandler | SyncHandler],
        tg: TaskGroup,
    ):

        self._logger = logging.getLogger(self.__class__.__name__)
        self.worker_id = worker_id
        """A unique id for worker. Only for identification."""

        self.signaling_url = signaling_url
        """The signaling server url should not contain the protocol."""

        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        """The websocket connection to the signaling server. Use to send offer."""

        self.hooks = {name: Peer.jsonargs(hook) for name, hook in hooks.items()}
        """Every time a new channel is created by peer (not by our), this handler will be called."""

        self.tg = tg

        self.ice_configs = ice_configs

        self.conns: dict[str, PeerConnection] = {}
        self.lo = SelfConnection(worker_id, self.hooks, tg)
        self.lock = anyio.Lock()

        self.tg.start_soon(self._register)

    @staticmethod
    def jsonargs(
        func: Callable[..., Any]
    ) -> Callable[..., Any | Coroutine[Any, Any, Any]]:
        @functools.wraps(func)
        def wrapper(*args: Any) -> Any | Coroutine[Any, Any, Any]:
            signature = inspect.signature(func)
            type_hints = get_type_hints(func)

            parsed_args = []
            for param_name, arg in zip(signature.parameters, args):
                model_cls = type_hints[param_name]
                if isinstance(arg, (str, bytes)):
                    parsed_arg = model_cls.model_validate_json(arg)
                elif isinstance(arg, dict):
                    parsed_arg = model_cls.model_validate(arg)
                else:
                    parsed_arg = arg
                parsed_args.append(parsed_arg)
            return func(*parsed_args)

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any) -> Any:
                return await wrapper(*args)

            return async_wrapper
        else:
            return wrapper

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
                    self.tg,
                )
            pc = self.conns[from_worker_id]
            await pc.create_answer(ws, request.sdp)

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
                            else:
                                self._logger.error(
                                    "Unknown message type %s",
                                    message.__class__.__name__,
                                )
            except Exception as e:
                self._logger.warn("Failed to connect to signaling server due to %s", e)
                await anyio.sleep(1)

    async def connect(self, to_worker_id: str) -> Connection:
        if to_worker_id == self.worker_id:
            return self.lo

        async with self.lock:
            if to_worker_id not in self.conns:
                self.conns[to_worker_id] = PeerConnection(
                    self.worker_id, to_worker_id, self.ice_configs, self.hooks, self.tg
                )
            pc = self.conns[to_worker_id]

        await pc.create_offer(self.ws)

        return pc
