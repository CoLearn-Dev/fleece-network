from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from typing import Callable, Optional

from .fleece_network_rust import (  # type: ignore
    PyCodecRequest,
    PyCodecResponse,
    PyProxy,
    PyProxyBuilder,
    PyRequestId,
)


class Peer:
    def __init__(
        self,
        center_addr: Optional[str],
        center_peer_id: Optional[str],
        self_addr: Optional[str],
        handlers: dict[str, Callable[[bytes], str]],
    ):
        builder = (
            PyProxyBuilder()
            .center(
                center_addr,
                center_peer_id,
            )
            .this(self_addr)
        )
        self.proxy: PyProxy = builder.build()
        self.handlers = handlers
        self.pool = ThreadPoolExecutor(max_workers=16)

    def peer_id(self) -> str:
        return self.proxy.peer_id()

    def send(self, peer_id: str, route: str, payload: bytes) -> tuple[str, bytes]:
        response = self.proxy.send_request(
            peer_id,
            PyCodecRequest(
                route,
                payload,
            ),
        )
        return response.status, response.payload

    def run(self):
        thread = Thread(target=self.listen)
        thread.start()

    def listen(self):
        while True:
            r = self.proxy.recv()
            if r is not None:
                request_id, request = r
                self.pool.submit(self._delegate, request_id, request)
            else:
                break

    def enable_log(self):
        self.proxy.enable_log()

    def _delegate(self, request_id: PyRequestId, request: PyCodecRequest):
        response = self.handlers[request.route](bytes(request.payload))
        response = PyCodecResponse("Ok", response.encode())
        self.proxy.send_response(request_id, response)
