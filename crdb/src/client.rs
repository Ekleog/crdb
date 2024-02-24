mod api_db;
mod client_db;
mod connection;
mod obj;

pub use api_db::ApiDb;
pub use client_db::{ClientDb, ClientVacuumSchedule};
pub use connection::ConnectionEvent;
pub use obj::Obj;

#[cfg(target_arch = "wasm32")]
pub use crdb_indexed_db::IndexedDb as LocalDb;
#[cfg(not(target_arch = "wasm32"))]
pub use crdb_sqlite::SqliteDb as LocalDb;
