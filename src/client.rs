mod api_db;
mod client_db;
mod config;
mod connection;
mod obj;

#[cfg(target_arch = "wasm32")]
mod indexed_db;
#[cfg(not(target_arch = "wasm32"))]
mod sqlite_db;

pub use api_db::{ApiDb, OnError};
pub use client_db::{ClientDb, ClientStorageInfo, ClientVacuumSchedule};
pub use connection::ConnectionEvent;
pub use obj::Obj;

#[cfg(target_arch = "wasm32")]
pub use indexed_db::IndexedDb as LocalDb;
#[cfg(not(target_arch = "wasm32"))]
pub use sqlite_db::SqliteDb as LocalDb;

use crate::{SessionToken, User};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LoginInfo {
    url: Arc<String>,
    user: User,
    token: SessionToken,
}
