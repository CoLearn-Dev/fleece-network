import asyncio
from dataclasses import dataclass
import pickle
import websockets
import logging
import logging.config
from typing import Any, Callable, Coroutine, Dict, List
from peerrtc.messages import ControlMessage, RegisterMessage, ConnectMessage
from aiortc import (
    RTCPeerConnection,
    RTCConfiguration,
    RTCIceServer,
    RTCDataChannel,
)


@dataclass
class TurnConfig:
    ip: str
    port: int
    username: str
    credential: str

    def to_ice_server(self) -> RTCIceServer:
        return RTCIceServer(
            f"turn:{self.ip}:{self.port}?transport=udp", self.username, self.credential
        )


class OutwardDataChannel:
    def __init__(self, channel: RTCDataChannel):
        self._logger = logging.getLogger(__name__)
        self.channel = channel
        self.read_queue = asyncio.Queue()

        @channel.on("open")
        async def on_open():
            self._logger.info("Data channel opened: %s", self.label())

        @channel.on("message")
        async def on_message(message: str):
            self._logger.info("Channel %s receives message: %s", self.label(), message)
            await self.read_queue.put(message)

    def label(self) -> str:
        return self.channel.label

    async def recv(self):
        return await self.read_queue.get()

    def send(self, message: str):
        self._logger.info("Channel %s sends message: %s", self.label(), message)
        self.channel.send(message)


class InwardDataChannel:
    def __init__(self, channel: RTCDataChannel):
        self._logger = logging.getLogger(__name__)
        self.channel = channel
        self.hooks: Dict[str, Callable[[str], Coroutine[Any, Any, None]]] = {}
        self.lock = asyncio.Lock()

        @channel.on("open")
        async def on_open():
            self._logger.info("Data channel opened: %s", self.label())

        @channel.on("message")
        async def on_message(message: str):
            self._logger.info("Channel %s receives message: %s", self.label(), message)
            message: ControlMessage = pickle.loads(message)
            async with self.lock:
                callback = self.hooks.get(message.op)
                if callback is not None:
                    asyncio.create_task(callback(message.data))
                else:
                    self._logger.warning("No hook for operation: %s", message.op)

    def label(self) -> str:
        return self.channel.label

    def send(self, message: str):
        self._logger.info("Channel %s sends message: %s", self.label(), message)
        self.channel.send(message)

    async def on(self, hook: str, callback: Callable[[str], Coroutine[Any, Any, None]]):
        async with self.lock:
            self.hooks[hook] = callback
            self._logger.info("Hook %s added", hook)


class PeerConnection:
    def __init__(
        self,
        pc: RTCPeerConnection,
        channel_handler: Callable[
            [InwardDataChannel], Coroutine[Any, Any, None]
        ] = None,
        out_channel: OutwardDataChannel | None = None,
    ):
        """
        Create a peer connection with given RTCPeerConnection and DataChannel.

        Only when the peer connection is established by the current peer, the `DataChannel` should be provided.
        """

        self._logger = logging.getLogger(__name__)
        self.pc = pc
        self.in_channels: Dict[str, InwardDataChannel] = {}
        self.out_channels: Dict[str, OutwardDataChannel] = {}
        self.lock = asyncio.Lock()

        if out_channel is not None:
            self.out_channels[out_channel.label] = out_channel

        @pc.on("datachannel")
        async def on_datachannel(channel: RTCDataChannel):
            async with self.lock:
                in_channel = InwardDataChannel(channel)
                self.in_channels[in_channel.label()] = in_channel
            await channel_handler(in_channel)
            self._logger.info("Data channel created: %s", in_channel.label())

    async def create(self, label: str) -> OutwardDataChannel | None:
        """Create a data channel with given label."""
        async with self.lock:
            if label in self.out_channels:
                return self.out_channels[label]
            elif label in self.in_channels:
                return None
            else:
                out_channel = OutwardDataChannel(self.pc.createDataChannel(label))
                self.out_channels[label] = out_channel
                return out_channel

    async def close(self, label: str):
        """Close a data channel with given label."""
        out_channel = None
        async with self.lock:
            if label in self.out_channels:
                out_channel = self.out_channels.pop(label)
        if out_channel is not None:
            await out_channel.close()


class Peer:
    def __init__(
        self,
        worker_id: str,
        signaling_url: str,
        turn_configs: List[TurnConfig],
        stun_url: str,
        channel_handler: Callable[[InwardDataChannel], Coroutine[Any, Any, None]],
    ):
        self._logger = logging.getLogger(__name__)
        self.worker_id = worker_id
        """A unique id for worker. Only for identification."""

        self.signaling_url = signaling_url
        """The signaling server url should not contain the protocol."""

        self.ws: websockets.WebSocketClientProtocol = None
        """The websocket connection to the signaling server. Use to send offer."""

        self.channel_handler = channel_handler
        """Every time a new channel is created by peer (not by our), this handler will be called."""

        self.turn_configs = turn_configs
        self.stun_url = stun_url

        self.waiting_sdps: Dict[str, asyncio.Queue] = {}
        self.peer_conns: Dict[str, PeerConnection] = {}
        self.lock = asyncio.Lock()

        asyncio.create_task(self._register())

    async def _register(self):
        while True:
            async with websockets.connect(f"ws://{self.signaling_url}/register") as ws:
                async with self.lock:
                    self.ws = ws
                self._logger.info("Connecting to signaling server")

                # send register message first
                await ws.send(pickle.dumps(RegisterMessage(self.worker_id)))
                self._logger.info("Registering worker: %s", self.worker_id)

                try:
                    while True:
                        raw = await ws.recv()
                        message: ConnectMessage = pickle.loads(raw)
                        if message.sdp.type == "answer":
                            async with self.lock:
                                signal = self.waiting_sdps.pop(
                                    message.from_worker_id, None
                                )
                                if signal == None:
                                    self._logger.warning("No signal queue for answer")
                                    continue
                            await signal.put(message.sdp)
                        else:  # it's an offer
                            self._logger.info(
                                "Received offer from %s", message.from_worker_id
                            )
                            pc = RTCPeerConnection(
                                configuration=RTCConfiguration(
                                    [RTCIceServer(self.stun_url)]
                                    + [
                                        config.to_ice_server()
                                        for config in self.turn_configs
                                    ]
                                )
                            )
                            await pc.setRemoteDescription(message.sdp)
                            await pc.setLocalDescription(await pc.createAnswer())
                            answermsg = ConnectMessage(
                                self.worker_id,
                                message.from_worker_id,
                                pc.localDescription,
                            )
                            await ws.send(pickle.dumps(answermsg))
                            self._logger.info(
                                "Sent answer to %s", message.from_worker_id
                            )

                            async with self.lock:
                                self.peer_conns[message.from_worker_id] = (
                                    PeerConnection(pc, self.channel_handler)
                                )
                except websockets.exceptions.ConnectionClosed:
                    self._logger.info("Connection closed")
                    async with self.lock:
                        self.ws = None

    async def connect(
        self, to_worker_id: str, label: str
    ) -> tuple[PeerConnection, OutwardDataChannel | None] | None:
        """Please don't connect to the same worker simultaneously. It's not supported yet."""

        async with self.lock:
            if to_worker_id in self.peer_conns:
                existing_pc = self.peer_conns[to_worker_id]
                if label in existing_pc.out_channels:
                    return existing_pc, existing_pc.out_channels[label]
                elif label in existing_pc.in_channels:
                    return existing_pc, None
                else:
                    out_channel = await existing_pc.create(label)
                    return existing_pc, out_channel

            ws = self.ws
            if ws is None:
                self._logger.warning("Not connected to signaling server")
                return None

        # establish connection
        pc = RTCPeerConnection(
            configuration=RTCConfiguration(
                [RTCIceServer(self.stun_url)]
                + [config.to_ice_server() for config in self.turn_configs]
            )
        )
        channel = pc.createDataChannel(
            label
        )  # create data channel otherwise the connection cannot be established

        # signal is created to receive answer from websocket, because we should not handle `recv()` here
        signal = asyncio.Queue()
        async with self.lock:
            self.waiting_sdps[to_worker_id] = signal

        await pc.setLocalDescription(await pc.createOffer())
        offermsg = ConnectMessage(self.worker_id, to_worker_id, pc.localDescription)
        await ws.send(pickle.dumps(offermsg))

        self._logger.info("Sent offer to %s", to_worker_id)

        answer = await signal.get()
        await pc.setRemoteDescription(answer)

        self._logger.info("Connected to %s", to_worker_id)

        async with self.lock:
            out_channel = OutwardDataChannel(channel)
            new_pc = PeerConnection(pc, self.channel_handler, out_channel)
            self.peer_conns[to_worker_id] = new_pc
            return new_pc, out_channel
