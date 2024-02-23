mod cache;
mod importance;

#[cfg(all(test, not(feature = "_tests")))]
const _: () = panic!("running tests without the `_tests` feature enabled");

pub use crdb_core::*;
pub use crdb_macros::*;

pub use importance::Importance;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::{ClientDb, ClientVacuumSchedule, ConnectionEvent, Obj};
#[cfg(not(feature = "client"))]
mod client {
    #[macro_export]
    macro_rules! generate_client {
        ($($_:tt)*) => {};
    }
}

#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
pub use server::{Server, ServerVacuumSchedule};
#[cfg(not(feature = "server"))]
mod server {
    #[macro_export]
    macro_rules! generate_server {
        ($($_:tt)*) => {};
    }
}

// Stuff used by macros
// TODO(misc-high): verify all the stuff here is actually required (and review the whole pub crate api too)
#[doc(hidden)]
pub mod crdb_internal {
    #[cfg(feature = "client")]
    pub use crate::client::{
        ClientDb, ClientStorageInfo, ClientVacuumSchedule, ConnectionEvent, LocalDb, Obj, OnError,
    };
    #[cfg(feature = "server")]
    pub use crate::server::{PostgresDb, UpdatesMap};
    pub use crate::{cache::ObjectCache, *};
    pub use anyhow;
    pub use crdb_helpers;
    pub use futures::{self, channel::mpsc, future, stream, FutureExt, Stream};
    #[cfg(feature = "client")]
    pub use paste::paste;
    pub use serde;
    pub use serde_json;
    pub use std::{
        collections::{HashMap, HashSet},
        future::Future,
        ops::Bound,
        sync::{Arc, Mutex},
    };
    pub use tokio::{self, sync::oneshot};
    pub use tracing;
    pub use ulid::{self, Ulid};
}

#[cfg(not(target_arch = "wasm32"))]
pub use sqlx;

#[cfg(feature = "server")]
pub use cron;

pub use chrono;
pub use serde;
pub use serde_json;
pub use tokio::sync::broadcast;
