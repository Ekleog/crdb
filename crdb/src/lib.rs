mod api;
mod cache;
pub mod fts;
mod importance;
mod messages;
mod query;
mod session;
#[cfg(feature = "_tests")]
pub mod test_utils;
mod timestamp;

#[cfg(all(test, not(feature = "_tests")))]
const _: () = panic!("running tests without the `_tests` feature enabled");

use std::{any::Any, sync::Arc};

pub use crdb_core::*;

pub use importance::Importance;
pub use query::{JsonPathItem, Query};
pub use session::{NewSession, Session};

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
    pub use crate::server::{
        ComboLock, PostgresDb, ReadPermsChanges, ServerConfig, UpdatesMap, UpdatesWithSnap,
    };
    #[cfg(feature = "_tests")]
    pub use crate::test_utils;
    pub use crate::{
        api::{ApiConfig, UploadId},
        cache::ObjectCache,
        messages::{Request, Update, UpdateData, Updates, Upload},
        session::Session,
        *,
    };
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
pub use tokio::sync::broadcast;

// This module needs to actually be public, because the `generate` macros need to be
// able to implement the traits. However, making it doc(hidden) makes it look as though
// it is actually sealed in the documentation, which is good because we don't want
// users to rely on any stability guarantees there.
#[doc(hidden)]
pub mod private {
    pub trait Sealed {}
}

pub trait DynSized: 'static + Any + Send + Sync + deepsize::DeepSizeOf {
    // TODO(blocked): remove these functions once rust supports trait upcasting:
    // https://github.com/rust-lang/rust/issues/65991#issuecomment-1869869919
    // https://github.com/rust-lang/rust/issues/119335
    fn arc_to_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn ref_to_any(&self) -> &(dyn Any + Send + Sync);
    fn deep_size_of(&self) -> usize {
        <Self as deepsize::DeepSizeOf>::deep_size_of(self)
    }
}
impl<T: 'static + Any + Send + Sync + deepsize::DeepSizeOf> DynSized for T {
    fn arc_to_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn ref_to_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
}

pub fn hash_binary(data: &[u8]) -> BinPtr {
    use sha3::Digest;
    let mut hasher = sha3::Sha3_224::new();
    hasher.update(data);
    BinPtr(ulid::Ulid::from_bytes(
        hasher.finalize()[..16].try_into().unwrap(),
    ))
}

pub fn check_string(s: &str) -> crate::Result<()> {
    if s.contains('\0') {
        return Err(crate::Error::NullByteInString);
    }
    Ok(())
}

pub fn check_strings(v: &serde_json::Value) -> crate::Result<()> {
    match v {
        serde_json::Value::Null => (),
        serde_json::Value::Bool(_) => (),
        serde_json::Value::Number(_) => (),
        serde_json::Value::String(s) => check_string(s)?,
        serde_json::Value::Array(a) => {
            for v in a.iter() {
                check_strings(v)?;
            }
        }
        serde_json::Value::Object(m) => {
            for (k, v) in m.iter() {
                check_string(k)?;
                check_strings(v)?;
            }
        }
    }
    Ok(())
}

#[macro_export]
macro_rules! db {
    (
        // TODO(api-high): have a single `config` type defined here, by using a ServerDb trait and not directly PostgresDb
        $v:vis mod $module:ident {
            api_config: $api_config:ident,
            server_config: $server_config:ident,
            objects: {
                $( $name:ident : $object:ty, )*
            },
        }
    ) => {
        #[allow(unused_imports)]
        $v mod $module {

            use $crate::crdb_internal as crdb;
            use crdb::crdb_helpers;
            use crdb::Db as CrdbDb;
            use crdb::ResultExt as CrdbResultExt;
            use crdb::stream::StreamExt as CrdbStreamExt;

            $crate::generate_api!($api_config | $($object),*);
            $crate::generate_server!($api_config | $server_config | $($object),*);
        }
    }
}
