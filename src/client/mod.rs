mod api_db;
mod client_db;
mod config;
#[cfg(target = "wasm32-unknown-unknown")]
mod indexed_db;
#[cfg(not(target = "wasm32-unknown-unknown"))]
mod sqlite_db;

pub use api_db::ApiDb;
pub use client_db::ClientDb;
pub use config::{NewEvent, NewObject, NewSnapshot};
#[cfg(target = "wasm32-unknown-unknown")]
pub use indexed_db::IndexedDb as LocalDb;
#[cfg(not(target = "wasm32-unknown-unknown"))]
pub use sqlite_db::SqliteDb as LocalDb;

#[doc(hidden)]
pub trait Authenticator:
    'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize
{
}
impl<T: 'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize> Authenticator
    for T
{
}
