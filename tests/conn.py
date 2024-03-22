import logging
import logging.config
import anyio
import toml
import aioconsole  # type: ignore

logging.config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)

from peerrtc.peer import Peer


async def delegator(peer: Peer):
    peer_conn = None
    while True:
        try:
            input: str = await aioconsole.ainput()
            op, *args = input.split(" ")
            if op == "connect":
                peer_conn = await peer.connect(args[0])
            elif op == "send":
                if peer_conn is not None:
                    await peer_conn.send("hello", args[0])
                else:
                    logger.warning("No channel")
            else:
                logger.warning("Unknown operation")
        except Exception as e:
            logger.warning("Some exception: ", e)


async def main():
    config = toml.load("config.toml")

    async def cat(data):
        logger.info(data)
        return "ok", None

    hooks = {"hello": cat}

    try:
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
            hooks=hooks,
        )
    except Exception as e:
        logger.warning("Exception: %s", e)

    await delegator(peer)


if __name__ == "__main__":
    anyio.run(main)
