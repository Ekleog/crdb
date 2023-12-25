mod api_db;
mod client_db;
mod config;

pub use api_db::ApiDb;
pub use client_db::ClientDb;
pub use config::{NewEvent, NewObject, NewSnapshot};

#[doc(hidden)]
pub trait Authenticator:
    'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize
{
}
impl<T: 'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize> Authenticator
    for T
{
}
