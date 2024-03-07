from dataclasses import dataclass
from typing import Any, Dict
from aiortc import RTCSessionDescription


@dataclass
class RegisterMessage:
    worker_id: str


@dataclass
class ConnectMessage:
    from_worker_id: str
    to_worker_id: str
    sdp: RTCSessionDescription


@dataclass
class ControlMessage:
    op: str
    data: Any
