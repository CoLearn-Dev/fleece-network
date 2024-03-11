import asyncio
from typing import Any
import toml
import logging
import logging.config
import aioconsole
import time
import pickle
from peerrtc.peer import InwardDataChannel, Peer, TurnConfig
from peerrtc.messages import ControlMessage

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


async def delegator(peer: Peer):
    channel = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                _, channel = await peer.connect(args[0], f"{peer.worker_id}_rtt")
            elif op == "send":
                if channel is not None:
                    channel.send(pickle.dumps(ControlMessage("rtt", time.time())))
                    reply = pickle.loads(await channel.recv())
                    logger.info(time.time() - reply)
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Unknown exception: ", e)


async def main():
    config = toml.load("config.toml")

    async def handler(channel: InwardDataChannel):
        async def echo(data: Any):
            try:
                channel.send(pickle.dumps(data))
            except Exception as e:
                logger.warn("Get exception when handling: %s", e)

        await channel.on("rtt", echo)

    peer = Peer(
        worker_id=config["worker"]["id"],
        signaling_url="{}:{}".format(
            config["signaling"]["ip"], config["signaling"]["port"]
        ),
        turn_configs=[TurnConfig(**subconfig) for subconfig in config["turn"]],
        stun_url="stun:{}:{}".format(config["stun"]["ip"], config["stun"]["port"]),
        channel_handler=handler,
    )

    await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
