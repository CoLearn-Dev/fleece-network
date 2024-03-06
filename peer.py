import asyncio
import websockets
import json
import toml
import logging
import logging.config
import aioconsole
from typing import Any, Callable, Coroutine, Dict
from messages import ControlMessage, RegisterMessage, ConnectMessage
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
            logger.info("Data channel opened")

        @channel.on("message")
        async def on_message(message):
            await self.read_queue.put(message)

    async def recv(self):
        return await self.read_queue.get()

    def send(self, message):
        self.channel.send(message)


class Substitute:
    def __init__(
        self,
        pc: RTCPeerConnection,
        hooks: Dict[str, Callable[[str], Coroutine[Any, Any, None]]],
    ):
        self.pc = pc

        @pc.on("datachannel")
        def on_datachannel(channel):
            @channel.on("message")
            async def on_message(message: str):
                try:
                    message = ControlMessage.from_json(json.loads(message))
                except Exception as e:
                    logger.warning("Unknown message: ", e)
                    return

                try:
                    await hooks.get(message.op)(message.data)
                except KeyError:
                    logger.warning("Unknown operation: %s", message.op)


class Peer:
    def __init__(
        self,
        worker_id: str,
        signaling_url: str,
        turn_url: str,
        turn_username: str,
        turn_credential: str,
        stun_url: str = "stun:stun.l.google:19302",
        hooks: Dict[str, Callable[[str], Coroutine[Any, Any, Any]]] = {},
    ):
        self.worker_id = worker_id
        """A unique id for worker. Only for identification."""

        self.signaling_url = signaling_url
        """The signaling server url should not contain the protocol."""

        self.ws: websockets.WebSocketClientProtocol = None
        """The websocket connection to the signaling server. Use to send offer."""

        self.turn_url = turn_url
        self.turn_username = turn_username
        self.turn_credential = turn_credential
        self.stun_url = stun_url
        self.hooks = hooks

        self.waiting_sdps: Dict[str, asyncio.Queue] = {}
        self.substitutes: Dict[str, Substitute] = {}
        self.lock = asyncio.Lock()

    async def register(self):
        while True:
            async with websockets.connect(f"ws://{self.signaling_url}/register") as ws:
                async with self.lock:
                    self.ws = ws
                logger.info("Connecting to signal")

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
                                try:
                                    signal = self.waiting_sdps.pop(
                                        message.from_worker_id
                                    )
                                except KeyError:
                                    logger.warning("KeyError")
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
                            self.substitutes[message.from_worker_id] = Substitute(
                                pc, self.hooks
                            )

                except websockets.exceptions.ConnectionClosed:
                    logger.info("Connection closed")
                    async with self.lock:
                        self.ws = None

    async def connect(self, to_worker_id: str) -> DataChannel | None:
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
        channel = pc.createDataChannel("data")

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

        return DataChannel(channel)

    def on(self, event: str, handler: Callable[[str], Coroutine[Any, Any, None]]):
        self.hooks[event] = handler


async def delegator(peer: Peer):
    channel = None
    while True:
        input: str = await aioconsole.ainput()
        op, *args = input.split(" ")
        if op == "connect":
            channel = await peer.connect(args[0])
            logger.info("Connected")
        elif op == "send":
            if channel is not None:
                channel.send(json.dumps(ControlMessage("hello", args[0]).to_json()))
            else:
                logger.warning("No channel")
        else:
            logger.warning("Unknown operation")


async def main():
    config = toml.load("config.toml")
    peer = Peer(
        worker_id=config["worker"]["id"],
        signaling_url="{}:{}".format(
            config["signaling"]["ip"], config["signaling"]["port"]
        ),
        turn_url="turn:{}:{}".format(config["turn"]["ip"], config["turn"]["port"]),
        turn_username=config["turn"]["username"],
        turn_credential=config["turn"]["credential"],
        stun_url="stun:{}:{}".format(config["stun"]["ip"], config["stun"]["port"]),
    )

    async def cat(data):
        logger.info(data)

    peer.on("hello", cat)
    asyncio.create_task(peer.register())

    await delegator(peer)


if __name__ == "__main__":
    logger.warning("test")
    asyncio.run(main())
