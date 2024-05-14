import pickle
from typing import Any

from safetensors.torch import load, save


def dumps(tensors: dict[str, "Tensor"], metadata: dict[str, Any]) -> bytes:
    metadata_bytes = pickle.dumps(metadata)
    tensors_bytes = save(tensors)
    return (
        len(metadata_bytes).to_bytes(4, byteorder="big")
        + metadata_bytes
        + tensors_bytes
    )


def loads(b: bytes) -> tuple[dict[str, "Tensor"], dict[str, Any]]:
    metadata_length = int.from_bytes(b[:4], byteorder="big")
    metadata = pickle.loads(b[4 : 4 + metadata_length])
    tensors = load(b[4 + metadata_length :])
    return tensors, metadata
