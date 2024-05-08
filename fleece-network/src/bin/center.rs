use std::error::Error;

use fleece_network::center::Center;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread", worker_threads = 32)]
async fn main() -> Result<(), Box<dyn Error>> {
    // env_logger::Builder::from_default_env().init();
    let _ = tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_file(true)
                .with_line_number(true),
        )
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let addr = "/ip4/0.0.0.0/tcp/9765";
    let authority = Center::new(&[addr.parse().unwrap()]);
    authority.run().await;
    Ok(())
}
