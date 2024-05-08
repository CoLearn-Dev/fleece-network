use futures::future::Either;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed, upgrade::Version},
    dns,
    identity::Keypair,
    noise, quic, tcp, websocket, yamux, PeerId, Transport,
};
use libp2p_mplex::MplexConfig;

pub(crate) fn generate_tcp_transport(keypair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let noise_config = noise::Config::new(keypair).unwrap();
    let multiplex_config = {
        let mplex_config = MplexConfig::default();
        let yamux_config = yamux::Config::default();
        libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
    };
    let tcp = tcp::tokio::Transport::new(tcp::Config::default().port_reuse(true).nodelay(true));
    tcp.upgrade(Version::V1Lazy)
        .authenticate(noise_config)
        .multiplex(multiplex_config)
        .boxed()
}

// pub(crate) fn generate_ws_tcp_transport(keypair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
//     let noise_config = noise::Config::new(keypair).unwrap();
//     let multiplex_config = {
//         let mplex_config = MplexConfig::default();
//         let yamux_config = yamux::Config::default();
//         libp2p::core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
//     };
//     let fallback =
//         tcp::tokio::Transport::new(tcp::Config::default().port_reuse(true).nodelay(true));
//     let tcp = tcp::tokio::Transport::new(tcp::Config::default().port_reuse(true).nodelay(true));
//     let ws = websocket::WsConfig::new(tcp).or_transport(fallback);
//     let up = ws
//         .upgrade(Version::V1Lazy)
//         .authenticate(noise_config)
//         .multiplex(multiplex_config)
//     let quic = quic::tokio::Transport::new(quic::Config::new(keypair)).or_transport(ws);
//     let dns = dns::tokio::Transport::system(quic).unwrap();
//     dns.map(|either, _| match either {
//         Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
//         Either::Right(either) => match either {
//             Either::Left((peer_id, muxer)) => todo!(),
//             Either::Right(_) => todo!(),
//         },
//     })
//     .boxed()
// }

// pub(crate) fn generate_quic_tcp_transport(keypair: &Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
//     let quic = quic::tokio::Transport::new(quic::Config::new(keypair));
//     dns::tokio::Transport::system(quic.or_transport(tcp))
//         .unwrap()
//         .map(|either, _| match either {
//             Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
//             Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
//         })
//         .boxed()
// }
