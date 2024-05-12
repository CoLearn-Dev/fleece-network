use std::time::Duration;

use libp2p::{
    futures::StreamExt,
    identify,
    identity::Keypair,
    ping, relay, rendezvous,
    swarm::{self, NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, Swarm,
};

use crate::transport::TransportBuilder;

pub struct Center {
    peer_id: PeerId,
    swarm: Swarm<Behaviour>,
}

impl Center {
    pub fn new(listen_addrs: &[Multiaddr]) -> Self {
        // let keypair = Keypair::generate_ed25519();
        let keypair = Keypair::ed25519_from_bytes([0u8; 32]).unwrap();
        let peer_id = keypair.public().to_peer_id();
        let transport_builder = TransportBuilder::new(keypair.clone())
            .with_tcp()
            .with_ws()
            .with_quic();
        let (transport_builder, relay_client) = transport_builder.with_relay();
        let transport = transport_builder.build();
        let behaviour = Behaviour::new(&keypair, relay_client);
        let swarm_config = swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(600));
        let mut swarm = Swarm::new(transport, behaviour, peer_id.clone(), swarm_config);

        listen_addrs.iter().for_each(|addr| {
            swarm.listen_on(addr.clone()).unwrap();
            swarm.add_external_address(addr.clone());
        });

        Self { peer_id, swarm }
    }

    pub async fn run(mut self) {
        loop {
            let event = self.swarm.next().await;
            match event {
                Some(event) => match event {
                    SwarmEvent::Behaviour(_) => {}
                    SwarmEvent::ConnectionEstablished { .. } => {}
                    SwarmEvent::ConnectionClosed { .. } => {}
                    SwarmEvent::IncomingConnection { .. } => {}
                    SwarmEvent::IncomingConnectionError { .. } => {}
                    SwarmEvent::OutgoingConnectionError { .. } => {}
                    SwarmEvent::NewListenAddr { .. } => {}
                    SwarmEvent::ExpiredListenAddr { .. } => {}
                    SwarmEvent::ListenerClosed { .. } => {}
                    SwarmEvent::ListenerError { .. } => {}
                    SwarmEvent::Dialing { .. } => {}
                    SwarmEvent::NewExternalAddrCandidate { .. } => {}
                    SwarmEvent::ExternalAddrConfirmed { .. } => {}
                    SwarmEvent::ExternalAddrExpired { .. } => {}
                    _ => todo!(),
                },
                None => break,
            }
        }
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    identify: identify::Behaviour,
    rendezvous: rendezvous::server::Behaviour,
    relay: relay::Behaviour,
    relay_client: relay::client::Behaviour,
    ping: ping::Behaviour,
}

impl Behaviour {
    pub fn new(keypair: &Keypair, relay_client: relay::client::Behaviour) -> Self {
        let mut relay_config = relay::Config::default();
        relay_config.max_circuit_bytes = 4096 * 8192 * 4;

        Self {
            identify: identify::Behaviour::new(identify::Config::new(
                "fleece/1.0.0".to_string(),
                keypair.public(),
            )),
            rendezvous: rendezvous::server::Behaviour::new(Default::default()),
            relay: relay::Behaviour::new(keypair.public().to_peer_id(), relay_config),
            relay_client,
            ping: ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(60))),
        }
    }
}
