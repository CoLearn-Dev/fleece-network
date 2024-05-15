pub mod behaviour;
pub mod codec;
pub mod handler;
pub mod message;

pub use behaviour::Behaviour;
pub use codec::Codec;
pub use message::*;

use std::time::Duration;

pub struct Config {
    pub request_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
        }
    }
}
