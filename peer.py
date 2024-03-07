import asyncio
import websockets
import json
import toml
import logging
import logging.config
import aioconsole
from typing import Any, Callable, Coroutine, Dict
from messages import HookMessage, RegisterMessage, ConnectMessage
from aiortc import (
    RTCPeerConnection,
    RTCConfiguration,
    RTCIceServer,
    RTCDataChannel,
)

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


class OutwardDataChannel:
    def __init__(self, channel: RTCDataChannel):
        self.channel = channel
        self.read_queue = asyncio.Queue()

        @channel.on("open")
        async def on_open():
            logger.info("Data channel opened: %s", self.label())

        @channel.on("message")
        async def on_message(message: str):
            logger.info("Channel %s receives message: %s", self.label(), message)
            await self.read_queue.put(message)

    def label(self) -> str:
        return self.channel.label

    async def recv(self):
        return await self.read_queue.get()

    def send(self, message: str):
        logger.info("Channel %s sends message: %s", self.label(), message)
        self.channel.send(message)


class InwardDataChannel:
    def __init__(self, channel: RTCDataChannel):
        self.channel = channel
        self.hooks: Dict[str, Callable[[str], Coroutine[Any, Any, None]]] = {}
        self.lock = asyncio.Lock()

        @channel.on("open")
        async def on_open():
            logger.info("Data channel opened: %s", self.label())

        @channel.on("message")
        async def on_message(message: str):
            logger.info("Channel %s receives message: %s", self.label(), message)
            message = HookMessage.from_json(json.loads(message))
            async with self.lock:
                callback = self.hooks.get(message.op)
                if callback is not None:
                    asyncio.create_task(callback(message.data))

    def label(self) -> str:
        return self.channel.label

    async def on(self, hook: str, callback: Callable[[str], Coroutine[Any, Any, None]]):
        async with self.lock:
            self.hooks[hook] = callback


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
            logger.info("Data channel created: %s", in_channel.label())

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
        turn_url: str,
        turn_username: str,
        turn_credential: str,
        stun_url: str,
        channel_handler: Callable[[InwardDataChannel], Coroutine[Any, Any, None]],
    ):
        self.worker_id = worker_id
        """A unique id for worker. Only for identification."""

        self.signaling_url = signaling_url
        """The signaling server url should not contain the protocol."""

        self.ws: websockets.WebSocketClientProtocol = None
        """The websocket connection to the signaling server. Use to send offer."""

        self.channel_handler = channel_handler
        """Every time a new channel is created by peer (not by our), this handler will be called."""

        self.turn_url = turn_url
        self.turn_username = turn_username
        self.turn_credential = turn_credential
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
                logger.info("Connecting to signaling server")

                # send register message first
                await ws.send(json.dumps(RegisterMessage(self.worker_id).to_json()))
                logger.info("Registering worker: %s", self.worker_id)

                try:
                    while True:
                        raw = await ws.recv()
                        message: ConnectMessage = ConnectMessage.from_json(
                            json.loads(raw)
                        )
                        if message.sdp.type == "answer":
                            async with self.lock:
                                signal = self.waiting_sdps.pop(
                                    message.from_worker_id, None
                                )
                                if signal == None:
                                    logger.warning("No signal queue for answer")
                                    continue
                            await signal.put(message.sdp)
                        else:  # it's an offer
                            logger.info(
                                "Received offer from %s", message.from_worker_id
                            )
                            pc = RTCPeerConnection(
                                configuration=RTCConfiguration(
                                    [
                                        RTCIceServer(self.stun_url),
                                        RTCIceServer(
                                            f"{self.turn_url}?transport=udp",
                                            self.turn_username,
                                            self.turn_credential,
                                        ),
                                    ]
                                )
                            )
                            await pc.setRemoteDescription(message.sdp)
                            await pc.setLocalDescription(await pc.createAnswer())
                            await ws.send(
                                json.dumps(
                                    ConnectMessage(
                                        self.worker_id,
                                        message.from_worker_id,
                                        pc.localDescription,
                                    ).to_json()
                                )
                            )
                            logger.info("Sent answer to %s", message.from_worker_id)

                            async with self.lock:
                                self.peer_conns[message.from_worker_id] = (
                                    PeerConnection(pc, self.channel_handler)
                                )
                except websockets.exceptions.ConnectionClosed:
                    logger.info("Connection closed")
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
                logger.warning("Not connected to signaling server")
                return None

        # establish connection
        pc = RTCPeerConnection(
            configuration=RTCConfiguration(
                [
                    RTCIceServer(self.stun_url),
                    RTCIceServer(
                        f"{self.turn_url}?transport=udp",
                        self.turn_username,
                        self.turn_credential,
                    ),
                ]
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
        try:
            await ws.send(
                json.dumps(
                    ConnectMessage(
                        self.worker_id, to_worker_id, pc.localDescription
                    ).to_json()
                )
            )
        except Exception as e:
            logger.warning("Some exception: ", e)
            return None

        logger.info("Sent offer to %s", to_worker_id)

        answer = await signal.get()
        await pc.setRemoteDescription(answer)

        logger.info("Connected to %s", to_worker_id)

        async with self.lock:
            out_channel = OutwardDataChannel(channel)
            new_pc = PeerConnection(pc, self.channel_handler, out_channel)
            self.peer_conns[to_worker_id] = new_pc
            return new_pc, out_channel


async def delegator(peer: Peer):
    peer_conn = None
    channel = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                peer_conn, channel = await peer.connect(
                    args[0], f"{peer.worker_id}_hook"
                )
            elif op == "new":
                channel = await peer_conn.create(f"{peer.worker_id}_{args[0]}")
                logger.info("New channel created")
            elif op == "send":
                if channel is not None:
                    channel.send(json.dumps(HookMessage("hello", args[0]).to_json()))
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Some exception: ", e)


async def main():
    config = toml.load("config.toml")

    async def handler(channel: InwardDataChannel):
        async def cat(data):
            logger.info(data)

        await channel.on("hello", cat)

    peer = Peer(
        worker_id=config["worker"]["id"],
        signaling_url="{}:{}".format(
            config["signaling"]["ip"], config["signaling"]["port"]
        ),
        turn_url="turn:{}:{}".format(config["turn"]["ip"], config["turn"]["port"]),
        turn_username=config["turn"]["username"],
        turn_credential=config["turn"]["credential"],
        stun_url="stun:{}:{}".format(config["stun"]["ip"], config["stun"]["port"]),
        channel_handler=handler,
    )

    await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
