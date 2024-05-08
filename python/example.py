import time
from threading import Thread

from fleece_network_rust import (  # type: ignore
    PyCodecRequest,
    PyCodecResponse,
    PyProxy,
    PyProxyBuilder,
)


def hello():
    print("Hello from Python!")


def recv(proxy: PyProxy):
    while True:
        r = proxy.recv()
        print(time.time())
        if r is not None:
            request_id, request = r
            print(time.time())
            proxy.send_response(request_id, PyCodecResponse("Ok", b"get"))
            # print(type(request_id), type(request))


builder = (
    PyProxyBuilder()
    .center(
        "/ip4/127.0.0.1/tcp/9765/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
        "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
    )
    .this("/ip4/0.0.0.0/tcp/0")
)
proxy = builder.build()


print("Peer ID:", proxy.peer_id())
print("Begin to listen")

thread = Thread(target=recv, args=(proxy,))
thread.start()

content = b"0" * 2 * 8192

while True:
    line = input()
    begin = time.time()
    print(begin)
    proxy.send_request(
        line,
        PyCodecRequest(
            "hello",
            content,
        ),
    )
    end = time.time()
    print(end)
    print("Time:", (end - begin) * 1000)
