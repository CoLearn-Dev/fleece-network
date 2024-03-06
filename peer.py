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


class DataChannel:
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


class Webhook:
    def __init__(self, channel: DataChannel):
        self.channel = channel
        self.hooks: Dict[str, Callable[[str], Coroutine[Any, Any, None]]] = {}
        self.lock = asyncio.Lock()
        asyncio.create_task(self.distribute())

    async def distribute(self):
        while True:
            message = await self.channel.recv()
            message = HookMessage.from_json(json.loads(message))
            async with self.lock:
                callback = self.hooks.get(message.op)
                if callback is not None:
                    asyncio.create_task(callback(message.data))

    async def on(self, hook: str, callback: Callable[[str], Coroutine[Any, Any, None]]):
        async with self.lock:
            self.hooks[hook] = callback


class PeerConnection:
    def __init__(
        self,
        pc: RTCPeerConnection,
        channel_handler: Callable[[DataChannel], Coroutine[Any, Any, None]] = None,
        channel: DataChannel | None = None,
    ):
        """
        Create a peer connection with given RTCPeerConnection and DataChannel.

        Only when the peer connection is established by the current peer, the `DataChannel` should be provided.
        """

        self.pc = pc
        self.channels: Dict[str, DataChannel] = {}
        self.lock = asyncio.Lock()

        if channel is not None:
            self.channels[channel.label] = channel

        @pc.on("datachannel")
        async def on_datachannel(channel: RTCDataChannel):
            async with self.lock:
                new_channel = DataChannel(channel)
                self.channels[new_channel.label()] = channel
            await channel_handler(new_channel)
            logger.info("Data channel created: %s", new_channel.label())

    async def create(self, label: str) -> DataChannel:
        """Create a data channel with given label."""
        async with self.lock:
            if label in self.channels:
                return self.channels[label]
            else:
                self.channels[label] = DataChannel(self.pc.createDataChannel(label))

    async def close(self, label: str):
        """Close a data channel with given label."""
        channel = None
        async with self.lock:
            if label in self.channels:
                channel = self.channels.pop(label)
        if channel is not None:
            await channel.close()


class Peer:
    def __init__(
        self,
        worker_id: str,
        signaling_url: str,
        turn_url: str,
        turn_username: str,
        turn_credential: str,
        stun_url: str,
        channel_handler: Callable[[DataChannel], Coroutine[Any, Any, None]],
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

    async def register(self):
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

    async def connect(self, to_worker_id: str, label: str) -> DataChannel | None:
        """Please don't connect to the same worker simultaneously. It's not supported yet."""

        async with self.lock:
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
            self.peer_conns[to_worker_id] = PeerConnection(
                pc, self.channel_handler, channel
            )
            return channel


async def delegator(peer: Peer):
    channel = None
    while True:
        input: str = await aioconsole.ainput()
        op, *args = input.split(" ")
        if op == "connect":
            channel = await peer.connect(args[0], "hook")
            logger.info("Connected")
        elif op == "send":
            if channel is not None:
                channel.send(json.dumps(HookMessage("hello", args[0]).to_json()))
            else:
                logger.warning("No channel")
        else:
            logger.warning("Unknown operation")


async def main():
    config = toml.load("config.toml")

    async def handler(channel: DataChannel):
        if channel.label() == "hook":

            async def cat(data):
                logger.info(data)

            hook = Webhook(channel)
            await hook.on("hello", cat)

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

    asyncio.create_task(peer.register())

    await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
