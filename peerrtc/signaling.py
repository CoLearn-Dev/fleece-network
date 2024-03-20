import asyncio
import uvicorn
import logging
import pickle
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from peerrtc.messages import (
    ConnectReply,
    ConnectRequest,
    RegisterReply,
    RegisterRequest,
)


class Signaling:
    workers: Dict[str, WebSocket] = {}
    lock: asyncio.Lock = asyncio.Lock()


signaling = Signaling()
app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.websocket("/register")
async def register(socket: WebSocket):
    await socket.accept()
    reg: RegisterRequest = pickle.loads(await socket.receive_bytes())
    logger.info("Registering worker: %s", reg.worker_id)

    signaling.workers[reg.worker_id] = socket

    # reply for acknowledgement
    await socket.send_bytes(pickle.dumps(RegisterReply("ok")))

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
            signaling.workers.pop(reg.worker_id)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8765,
    )
