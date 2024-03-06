from dataclasses import dataclass
from typing import Any, Dict
from aiortc import RTCSessionDescription


class Jsonable:
    def to_json(self):
        dict = self.__dict__.copy()
        dict["__class__"] = self.__class__.__name__
        return {
            key: value.to_json() if isinstance(value, Jsonable) else value
            for key, value in dict.items()
        }

    @staticmethod
    def from_json(data: Dict[str, Any]):
        data = data.copy()
        cls = globals()[data.pop("__class__")]
        content = {
            key: (
                Jsonable.from_json(value)
                if isinstance(value, dict) and "__class__" in value
                else value
            )
            for key, value in data.items()
        }
        return cls(**content)


@dataclass
class RegisterMessage(Jsonable):
    worker_id: str


@dataclass
class ConnectMessage(Jsonable):
    from_worker_id: str
    to_worker_id: str
    sdp: RTCSessionDescription

    def to_json(self) -> Dict[str, Any]:
        content = self.__dict__.copy()
        content["__class__"] = self.__class__.__name__
        content["sdp"] = {
            "type": self.sdp.type,
            "sdp": self.sdp.sdp,
            "__class__": self.sdp.__class__.__name__,
        }
        return content


@dataclass
class HookMessage(Jsonable):
    op: str
    data: str
