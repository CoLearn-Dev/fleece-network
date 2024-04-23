from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from fastapi import Response
from pydantic import BaseModel  # type: ignore

from .aiortc import RTCSessionDescription


@dataclass
class RegisterRequest:
    worker_id: str


@dataclass
class RegisterReply:
    status: str


@dataclass
class AddLatencyRequest:
    uuid2latency: dict[str, float]


@dataclass
class DelLatencyRequest:
    uuid: str


@dataclass
class ConnectRequest:
    from_worker_id: str
    to_worker_id: str
    sdp: RTCSessionDescription


@dataclass
class ConnectReply:
    from_worker_id: str
    to_worker_id: str
    sdp: Optional[RTCSessionDescription]


T = TypeVar("T", bound=BaseModel)


@dataclass
class SimpleRequest(Generic[T]):
    id: int
    op: str
    data: T


@dataclass
class SimpleReply:
    id: int
    data: Response
