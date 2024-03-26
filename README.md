# Nameless

## Installation

Install necessary packages using pip.

## Usage

Firstly, run `signaling.py` on a server with public IP.

Secondly, run TURN server on a server with public IP.

Thirdly, run `peer.py` on any machine. The configuration has to be set beforehand in `config.toml` as follows:

```toml
[worker]
id = "worker-id"

[signaling]
ip = "signaling-server-ip"
port = 8765

[[turn]]
ip = "turn-server-ip"
port = 3478
username = "turn-server-username"
credential = "turn-server-credential"


[stun]
ip = "stun-server-ip"
port = 19302
```

Fill the blank with your own configuration.

Then, `conn.py` is used to check connectivity. Use `connect <worker-id>` to connect to a peer in the console. After the connection is established, you can send messages to the peer using `send <message>`.

`rtt.py` is used to measure the round trip time. Use `send` to measure the round trip time to a peer after establishing the connnection by `connect <worker-id>`.

`bw.py` is used to measure the bandwidth. It's used the same way as `rtt.py`.
