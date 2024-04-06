# Examples

These files could be used to measure the round-trip time between two peers, which should be used like:

```bash
python3 rtt.py config.toml
```

The `config.toml` should look like this:

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

[[stun]]
ip = "stun-server-ip"
port = 19302
```
