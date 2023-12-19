mod api;
pub use api::{DbPtr, Object, User};

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;
