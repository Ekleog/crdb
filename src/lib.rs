mod api;
mod cache;
mod db_trait;
mod error;
mod full_object;
mod ids;
mod session;
#[cfg(test)]
mod test_utils;

pub use api::{CanDoCallbacks, DbPtr, Event, JsonNumber, JsonPathItem, Object, Query};
pub use db_trait::Timestamp;
pub use error::{Error, Result};
pub use ids::{BinPtr, EventId, ObjectId, TypeId, User};
pub use session::{NewSession, Session, SessionRef, SessionToken};

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::{NewEvent, NewObject, NewRecreation};
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
pub use server::{Authenticator, Server};
#[cfg(not(feature = "server"))]
mod server {
    #[macro_export]
    macro_rules! generate_server {
        ($($_:tt)*) => {};
    }
}

// Stuff used by macros
#[doc(hidden)]
pub mod crdb_internal {
    #[cfg(feature = "client")]
    pub use crate::client::ClientDb;
    #[cfg(feature = "server")]
    pub use crate::server::{ComboLock, PostgresDb, ServerConfig};
    pub use crate::{
        api::{parse_snapshot, ApiConfig, CanDoCallbacks},
        cache::{CacheConfig, ObjectCache},
        db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
        error::ResultExt,
        hash_binary, private, BinPtr, DbPtr, Error, EventId, Object, ObjectId, Query, Result,
        Timestamp, TypeId, User,
    };
    pub use anyhow;
    pub use futures::{self, future, stream, Stream};
    #[cfg(feature = "client")]
    pub use paste::paste;
    pub use serde_json;
    pub use std::{
        future::Future,
        ops::Bound,
        sync::{Arc, Mutex},
    };
    pub use ulid::{self, Ulid};
}

#[cfg(any(feature = "server", feature = "client-native"))]
pub use sqlx;

// This module needs to actually be public, because the `generate` macros need to be
// able to implement the traits. However, making it doc(hidden) makes it look as though
// it is actually sealed in the documentation, which is good because we don't want
// users to rely on any stability guarantees there.
#[doc(hidden)]
pub mod private {
    pub trait Sealed {}
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
        serde_json::Value::String(s) => check_string(&s)?,
        serde_json::Value::Array(a) => {
            for v in a.iter() {
                check_strings(&v)?;
            }
        }
        serde_json::Value::Object(m) => {
            for (k, v) in m.iter() {
                check_string(&k)?;
                check_strings(&v)?;
            }
        }
    }
    Ok(())
}

#[macro_export]
macro_rules! db {
    (
        $v:vis mod $module:ident {
            auth: $authenticator:ty,
            api_config: $api_config:ident,
            server_config: $server_config:ident,
            client_db: $client_db:ident,
            objects: {
                $( $name:ident : $object:ty, )*
            },
        }
    ) => {
        $v mod $module {
            use $crate::crdb_internal as crdb;
            use crdb::Db as CrdbDb;
            use crdb::ResultExt as CrdbResultExt;
            use crdb::stream::StreamExt as CrdbStreamExt;

            $crate::generate_api!($authenticator | $api_config | $($object),*);
            $crate::generate_client!($authenticator | $api_config | $client_db | $($name: $object),*);
            $crate::generate_server!($authenticator | $api_config | $server_config | $($object),*);
        }
    }
}
