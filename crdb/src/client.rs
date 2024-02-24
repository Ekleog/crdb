mod api_db;
mod client_db;
mod connection;
mod obj;

#[cfg(target_arch = "wasm32")]
mod indexed_db;

pub use api_db::{ApiDb, OnError};
pub use client_db::{ClientDb, ClientVacuumSchedule};
pub use connection::ConnectionEvent;
pub use obj::Obj;

#[cfg(not(target_arch = "wasm32"))]
pub use crdb_sqlite::SqliteDb as LocalDb;
#[cfg(target_arch = "wasm32")]
pub use indexed_db::IndexedDb as LocalDb;
