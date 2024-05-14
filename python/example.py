import time
from threading import Thread

from fleece_network import Peer


def hello(payload: bytes):
    return "Ok"


peer = Peer(
    "/ip4/127.0.0.1/tcp/9765/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
    "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
    "/ip4/0.0.0.0/tcp/0",
    {"hello": hello},
)

peer.run()

payload = b"0" * 8192 * 2

while True:
    line = input()
    begin = time.time()
    peer.send(line, "hello", payload)
    end = time.time()
    print("Time:", (end - begin) * 1000)
