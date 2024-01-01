mod api;
pub use api::{BinPtr, CanDoCallbacks, DbPtr, Event, JsonPathItem, Object, Query, User};

mod cache;
mod db_trait;
mod full_object;

#[cfg(test)]
mod test_utils;

pub use db_trait::Timestamp;

#[cfg(any(feature = "client"))]
mod client;
#[cfg(any(feature = "client"))]
pub use client::{NewEvent, NewObject, NewSnapshot};
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
    pub use crate::server::Config as ServerConfig;
    pub use crate::{
        api::{CanDoCallbacks, Config as ApiConfig},
        cache::{CacheConfig, ObjectCache},
        db_trait::{Db, DynNewEvent, DynNewObject, DynNewSnapshot, EventId, ObjectId},
        hash_binary, private, BinPtr, DbPtr, Object, Query, Timestamp,
    };
    pub use anyhow;
    pub use futures::{self, future, stream, Stream};
    #[cfg(feature = "client")]
    pub use paste::paste;
    pub use std::{
        future::Future,
        ops::Bound,
        sync::{Arc, Mutex},
    };
    pub use ulid::{self, Ulid};
}

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
    BinPtr {
        id: ulid::Ulid::from_bytes(hasher.finalize()[..16].try_into().unwrap()),
    }
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
            use crdb::anyhow::Context as CrdbContext;
            use crdb::Db as CrdbDb;
            use crdb::future::FutureExt as CrdbFutureExt;
            use crdb::stream::StreamExt as CrdbStreamExt;

            $crate::generate_api!($authenticator | $api_config | $($object),*);
            $crate::generate_client!($authenticator | $api_config | $client_db | $($name: $object),*);
            $crate::generate_server!($authenticator | $api_config | $server_config | $($object),*);
        }
    }
}
