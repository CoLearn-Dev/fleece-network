use std::str::FromStr;

use bytes::Bytes;
use fleece_network::{
    error::Error,
    peer::{codec, eventloop::Command, handler::Handler, peer::Peer},
};
use libp2p::{Multiaddr, PeerId};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::oneshot,
    time::Instant,
};
use tower::{service_fn, util::BoxService};
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Error> {
    let _ = tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
    let center_addr: Multiaddr =
        "/ip4/127.0.0.1/tcp/9765/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN"
            .parse()
            .unwrap();
    let center_peer_id =
        PeerId::from_str("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN").unwrap();

    let mut handler: Handler<codec::Request, codec::Response> = Handler::new();
    handler.add(
        "hello".to_string(),
        BoxService::new(service_fn(|req: codec::Request| async move {
            Ok(codec::Response::new(req.route, Bytes::from("world")))
        })),
    );

    let addr = "/ip4/0.0.0.0/tcp/0".parse().unwrap();
    let peer = Peer::new(center_addr.clone(), center_peer_id, addr, handler);
    let peer_id = peer.peer_id;
    println!("Peer ID: {}", peer_id);

    let command_tx = peer.command_tx.clone();
    tokio::spawn(peer.run());

    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        let request = codec::Request::new("hello".to_string(), Bytes::from(vec![0u8; 8192 * 2]));
        let (sender, receiver) = oneshot::channel();
        let start = Instant::now();
        // info!("Sending");
        command_tx
            .send(Command::Request {
                request,
                peer_id: PeerId::from_str(&line).unwrap(),
                sender,
            })
            .await
            .unwrap();
        tokio::spawn(async move {
            let _response = receiver.await.unwrap().unwrap();
            println!("{:?}", start.elapsed());
        });
    }
    Ok(())
}
