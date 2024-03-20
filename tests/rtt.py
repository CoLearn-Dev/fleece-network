import asyncio
from typing import Any
import toml
import logging
import logging.config
import aioconsole  # type: ignore
import time
from peerrtc.peer import Peer, IceConfig

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


async def delegator(peer: Peer):
    channel = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                channel = await peer.connect(args[0])
            elif op == "send":
                if channel is not None:
                    await channel.send("rtt", time.time())
                    reply = await channel.recv()
                    if reply is not None:
                        _, otime = reply
                        logger.info(time.time() - otime)
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Unknown exception: ", e)


async def main():
    config = toml.load("config.toml")

    async def echo(data: Any):
        return "ok", data

    peer = Peer(
        worker_id=config["worker"]["id"],
        signaling_url="{}:{}".format(
            config["signaling"]["ip"], config["signaling"]["port"]
        ),
        ice_configs=[IceConfig("turn", **subconfig) for subconfig in config["turn"]]
        + [IceConfig("stun", **subconfig) for subconfig in config["stun"]],
        hooks={"rtt": echo},
    )

    await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
