import logging
import logging.config
import asyncio
import pickle
import toml
import aioconsole
from peerrtc.peer import InwardDataChannel, Peer, IceConfig
from peerrtc.messages import ControlMessage

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)


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
                    channel.send(pickle.dumps(ControlMessage("hello", args[0])))
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

    try:
        peer = Peer(
            worker_id=config["worker"]["id"],
            signaling_url="{}:{}".format(
                config["signaling"]["ip"], config["signaling"]["port"]
            ),
            icw_configs=[IceConfig("turn", **subconfig) for subconfig in config["turn"]]
            + [IceConfig("stun", **subconfig) for subconfig in config["stun"]],
            channel_handler=handler,
        )
    except Exception as e:
        logger.warning("Exception: %s", e)

    await delegator(peer)


if __name__ == "__main__":
    asyncio.run(main())
