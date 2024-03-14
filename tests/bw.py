import asyncio
from typing import Any
import toml
import logging
import logging.config
import aioconsole
import time
import pickle
from peerrtc.peer import InwardDataChannel, Peer, IceConfig
from peerrtc.messages import ControlMessage

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

size = 16384
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


async def delegator(peer: Peer, test: SpeedTest):
    channel = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                _, channel = await peer.connect(args[0], f"{peer.worker_id}_rtt")
            elif op == "send":
                if channel is not None:
                    channel.send(ControlMessage("rtt", "start"))
                    tosend = pickle.dumps(ControlMessage("rtt", content))
                    test.start()

                    for _ in range(100):
                        channel.send(tosend)
                        test.go(size)

                    channel.send(pickle.dumps(ControlMessage("rtt", "end")))
                    logger.warning("Bandwidth: %s MB/s", test.end() / 1024 / 1024)
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Unknown exception: ", e)


async def main():
    config = toml.load("config.toml")
    test = SpeedTest()

    async def handler(channel: InwardDataChannel):
        async def sum(data: Any):
            if data == "start":
                test.start()
            elif data == "end":
                logger.warning("Bandwidth: %s MB/s", test.end() / 1024 / 1024)
            else:
                test.go(size)

        await channel.on("rtt", sum)

    peer = Peer(
        worker_id=config["worker"]["id"],
        signaling_url="{}:{}".format(
            config["signaling"]["ip"], config["signaling"]["port"]
        ),
        ice_configs=[IceConfig("turn", **subconfig) for subconfig in config["turn"]]
        + [IceConfig("stun", **subconfig) for subconfig in config["stun"]],
        channel_handler=handler,
    )

    await delegator(peer, test)


if __name__ == "__main__":
    asyncio.run(main())
