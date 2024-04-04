import asyncio
from typing import Any
from anyio import create_task_group
from pydantic import BaseModel
import toml
import logging
import logging.config
import aioconsole  # type: ignore
import time
from peerrtc.peer import Peer

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


class Message(BaseModel):
    data: Any


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
                    reply = await channel.send("rtt", Message(data=time.time()))
                    logger.info(
                        time.time()
                        - float(Message.model_validate_json(reply.body).data)
                    )
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Unknown exception: ", e)


async def main():
    config = toml.load("config.toml")

    async def echo(data: Message) -> Message:
        return data

    async with create_task_group() as tg:
        peer = Peer(
            worker_id=config["worker"]["id"],
            signaling_url="{}:{}".format(
                config["signaling"]["ip"], config["signaling"]["port"]
            ),
            ice_configs=[
                (
                    f"turn:{subconfig['ip']}:{subconfig['port']}",
                    subconfig["username"],
                    subconfig["credential"],
                )
                for subconfig in config["turn"]
            ]
            + [
                (f"turn:{subconfig['ip']}:{subconfig['port']}", None, None)
                for subconfig in config["stun"]
            ],
            hooks={"rtt": echo},
            tg=tg,
        )
        await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
