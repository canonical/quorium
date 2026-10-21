mod backup;
mod builder;
mod client;
pub mod discovery;
mod entity;
mod escrow;
mod escrow_field;
mod metrics;
mod node;
mod service;
mod singleton;
#[cfg(feature = "testing")]
pub mod testing;

#[doc(hidden)]
pub mod __private {
    pub use crate::private::*;
}

mod private;

pub use builder::{Builder, StartError, StateBackend};
pub use client::Client;
pub use discovery::Discovery;
pub use entity::{Entity, Error, Handle, Key, MigrationError, State, Watch};
pub use escrow::EscrowCounter;
pub use escrow_field::Escrow;
pub use node::{ClientError, Membership, Quorium, Stats, Status};
pub use quorium_core::entity::Ctx;
pub use quorium_macros::{State, state};
pub use quorium_net::tls::{TlsConfig, TlsMaterial};
pub use quorium_net::transport::Protocol as Transport;
pub use singleton::{Lease, Singleton};
