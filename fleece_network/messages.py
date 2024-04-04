from dataclasses import dataclass
from typing import Any, Generic, Optional, TypeVar
from aiortc import RTCSessionDescription  # type: ignore
from fastapi import Response
from pydantic import BaseModel  # type: ignore


@dataclass
class RegisterRequest:
    worker_id: str


@dataclass
class RegisterReply:
    status: str


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
