# Nameless

## Installation

Install necessary packages using pip.

## Usage

Firstly, run `signaling.py` on a server with public IP.

Secondly, run TURN server on a server with public IP.

Thirdly, run `peer.py` on any machine. The configuration has to be set beforehand in `config.toml` as follows:

```toml
[worker]
id = <worker-id>

[signaling]
ip = <signaling-server-ip>
port = <signaling-server-port>

[turn]
ip = <turn-server-ip>
port = <turn-server-port>
username = <turn-server-username>
credential = <turn-server-credential>


[stun]
ip = <stun-server-ip>
port = <stun-server-port>
```

Fill the blank with your own configuration.

Then, you can use `connect <worker-id>` to connect to a peer in the console. After the connection is established, you can send messages to the peer using `send <message>`.
