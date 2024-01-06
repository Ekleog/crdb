mod api_db;
mod client_db;
mod config;
#[cfg(not(feature = "client-native"))]
mod indexed_db;
#[cfg(feature = "client-native")]
mod sqlite_db;

pub use api_db::ApiDb;
pub use client_db::ClientDb;
pub use config::{NewEvent, NewObject, NewRecreation};
#[cfg(not(feature = "client-native"))]
pub use indexed_db::IndexedDb as LocalDb;
#[cfg(feature = "client-native")]
pub use sqlite_db::SqliteDb as LocalDb;

pub trait Authenticator:
    'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize
{
}
impl<T: 'static + Send + Sync + for<'de> serde::Deserialize<'de> + serde::Serialize> Authenticator
    for T
{
}
