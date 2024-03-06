import asyncio
import uvicorn
import logging
from typing import Dict
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from messages import ConnectMessage, RegisterMessage


class Signaling:
    workers: Dict[str, WebSocket] = {}
    lock: asyncio.Lock = asyncio.Lock()


signaling = Signaling()
app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.websocket("/register")
async def register(socket: WebSocket):
    await socket.accept()
    reg = RegisterMessage.from_json(await socket.receive_json())
    logger.info("Registering worker: %s", reg.worker_id)

    signaling.workers[reg.worker_id] = socket

    try:
        while True:
            raw_json = await socket.receive_json()
            message = ConnectMessage.from_json(raw_json)
            logger.info(
                "Receiving message from %s, sending to %s",
                message.from_worker_id,
                message.to_worker_id,
            )
            async with signaling.lock:
                worker = signaling.workers.get(message.to_worker_id)
                if worker != None:
                    await worker.send_json(raw_json)
                else:
                    logger.warn("No worker")
                    pass
    except WebSocketDisconnect:
        async with signaling.lock:
            signaling.workers.pop(reg.worker_id)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8765,
    )
