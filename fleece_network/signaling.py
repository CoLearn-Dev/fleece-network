import argparse
import asyncio
import csv
import logging
import pickle
from typing import Optional

import uvicorn
from anyio import create_task_group
from anyio.abc import TaskGroup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .messages import (
    AddLatencyRequest,
    ConnectReply,
    ConnectRequest,
    DelLatencyRequest,
    RegisterReply,
    RegisterRequest,
)


class Mapping:
    def __init__(self, num: int):
        self.num = num
        self.assigned_ids = [i for i in range(num)]
        self.id2uuid: dict[int, str] = {}
        self.uuid2id: dict[str, int] = {}

    def assign(self, uuid: str) -> tuple[Optional[int], list[str]]:
        if len(self.assigned_ids) == 0:
            return None, []
        affected = list(self.id2uuid.values())
        id = self.assigned_ids.pop()
        self.id2uuid[id] = uuid
        self.uuid2id[uuid] = id
        return (id, affected)

    def remove(self, uuid: str) -> tuple[Optional[int], list[str]]:
        if uuid not in self.uuid2id:
            return None, []

        id = self.uuid2id[uuid]
        self.uuid2id.pop(uuid)
        self.id2uuid.pop(id)
        self.assigned_ids.append(id)
        return id, list(self.id2uuid.values())


class Signaling:
    def __init__(self):
        self.id_mappings = Mapping(0)  # simply a placeholder, would be inited later
        self.latency_mappings: list[list[float]] = []
        self.workers: dict[str, WebSocket] = {}
        self.lock: asyncio.Lock = asyncio.Lock()


signaling = Signaling()
app = FastAPI()
logger = logging.getLogger("uvicorn.error")


async def sync_latency(new_uuid: str, affected_uuids: list[str], tg: TaskGroup):
    new_id = signaling.id_mappings.uuid2id[new_uuid]
    affected_ids = [signaling.id_mappings.uuid2id[uuid] for uuid in affected_uuids]
    new_socket = signaling.workers[new_uuid]
    tg.start_soon(
        new_socket.send_bytes,
        pickle.dumps(
            AddLatencyRequest(
                {
                    signaling.id_mappings.id2uuid[id]: signaling.latency_mappings[
                        new_id
                    ][id]
                    for id in affected_ids
                }
            )
        ),
    )

    for affected_id in affected_ids:
        affected_uuid = signaling.id_mappings.id2uuid[affected_id]
        affected_socket = signaling.workers[affected_uuid]
        tg.start_soon(
            affected_socket.send_bytes,
            pickle.dumps(
                AddLatencyRequest(
                    {
                        new_uuid: signaling.latency_mappings[affected_id][new_id],
                    }
                )
            ),
        )


@app.websocket("/register")
async def register(socket: WebSocket):
    await socket.accept()
    reg: RegisterRequest = pickle.loads(await socket.receive_bytes())
    uuid = reg.worker_id

    async with signaling.lock:
        if uuid not in signaling.workers:
            id, affected_workers = signaling.id_mappings.assign(uuid)
            if id is not None:
                logger.info("Registering worker %s", uuid)
                signaling.workers[uuid] = socket
                await socket.send_bytes(pickle.dumps(RegisterReply("ok")))
            else:
                logger.info("Cannot register worker %s due to full capacity", uuid)
                await socket.send_bytes(pickle.dumps(RegisterReply("full")))
                return
        else:
            logger.info("Duplicate worker %s", uuid)
            await socket.send_bytes(pickle.dumps(RegisterReply("duplicate")))
            return

    async with create_task_group() as tg:
        await sync_latency(uuid, affected_workers, tg)

    try:
        while True:
            raw = await socket.receive_bytes()
            message = pickle.loads(raw)
            logger.info(
                "Receiving message from %s, sending to %s",
                message.from_worker_id,
                message.to_worker_id,
            )
            async with signaling.lock:
                worker = signaling.workers.get(message.to_worker_id)
                if worker is not None:
                    asyncio.create_task(worker.send_bytes(raw))
                else:
                    logger.warning("No worker")
                    if isinstance(message, ConnectRequest):
                        asyncio.create_task(
                            socket.send_bytes(
                                pickle.dumps(
                                    ConnectReply(
                                        message.to_worker_id,
                                        message.from_worker_id,
                                        None,
                                    )
                                )
                            )
                        )

    except WebSocketDisconnect:
        async with signaling.lock:
            signaling.workers.pop(uuid)
            id, affected_uuids = signaling.id_mappings.remove(uuid)
            for affected_uuid in affected_uuids:
                affected_socket = signaling.workers[affected_uuid]
                await affected_socket.send_bytes(pickle.dumps(DelLatencyRequest(uuid)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config", type=str, required=True, help="Path to config file (csv)"
    )
    args = parser.parse_args()

    # get config
    csv_path = args.config
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        signaling.latency_mappings = [[float(item) for item in row] for row in reader]

    # sanity check
    n = len(signaling.latency_mappings)
    assert all(len(row) == n for row in signaling.latency_mappings)
    signaling.id_mappings = Mapping(n)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8765,
    )
