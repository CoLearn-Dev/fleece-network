import logging
import logging.config
import sys
import time
from typing import Any

import aioconsole  # type: ignore
import anyio
import toml
from anyio import create_task_group
from pydantic import BaseModel

from fleece_network.peer import Peer

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

size = 1200 * 1024
content = "a" * size


class SpeedTest:
    def __init__(self):
        self.bytes = 0.0
        self.start_time = 0.0

    def start(self):
        self.start_time = time.time()

    def end(self) -> float:
        bw = self.bytes / (time.time() - self.start_time)
        self.bytes = 0.0
        self.start_time = 0.0
        return bw

    def go(self, bytes: int):
        self.bytes += bytes


class Message(BaseModel):
    data: Any


async def delegator(peer: Peer, test: SpeedTest):
    channel = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                channel = await peer.connect(args[0])
            elif op == "send":
                if channel is not None:
                    await channel.send("rtt", b"start")
                    test.start()

                    for _ in range(10):
                        await channel.send("rtt", content.encode())
                        test.go(size)

                    await channel.send("rtt", b"ends")
                    logger.warning("Bandwidth: %s MB/s", test.end() / 1024 / 1024)
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Unknown exception: ", e)


async def main():
    config = toml.load(sys.argv[1])
    test = SpeedTest()

    async def sum(message: bytes):
        if message == b"start":
            test.start()
        elif message == b"end":
            logger.warning("Bandwidth: %s MB/s", test.end() / 1024 / 1024)
        else:
            test.go(size)
        return None

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
            hooks={"rtt": sum},
            tg=tg,
        )
        await delegator(peer, test)


if __name__ == "__main__":
    anyio.run(main)
