mod api_db;
mod client_db;
mod config;
mod connection;
#[cfg(target_arch = "wasm32")]
mod indexed_db;
#[cfg(not(target_arch = "wasm32"))]
mod sqlite_db;

pub use api_db::ApiDb;
pub use client_db::{ClientDb, ClientStorageInfo, ClientVacuumSchedule};
pub use connection::ConnectionEvent;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::IndexedDb as LocalDb;
#[cfg(not(target_arch = "wasm32"))]
pub use sqlite_db::SqliteDb as LocalDb;
