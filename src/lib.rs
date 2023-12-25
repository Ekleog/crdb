mod api;
pub use api::{BinPtr, CanDoCallbacks, DbPtr, JsonPathItem, Object, Query, User};

mod cache;
mod db_trait;
pub use db_trait::Timestamp;

#[cfg(feature = "client")]
mod client;
#[cfg(not(feature = "client"))]
mod client {
    #[macro_export]
    macro_rules! generate_client {
        ($($_:tt)*) => {};
    }
}
#[cfg(feature = "client")]
pub use client::{NewEvent, NewObject, NewSnapshot};

#[cfg(feature = "server")]
pub mod server;
#[cfg(not(feature = "server"))]
pub mod server {
    #[macro_export]
    macro_rules! generate_server {
        ($($_:tt)*) => {};
    }
}

// Stuff used by macros
#[doc(hidden)]
pub mod crdb_internal {
    pub use crate::{
        cache::{CacheConfig, ObjectCache},
        db_trait::{Db, NewEvent, NewObject, NewSnapshot},
        server, BinPtr, DbPtr, Object, Query, Timestamp,
    };
    pub use anyhow;
    pub use futures::{self, Stream};
    pub use paste::paste;
    pub use std::{future::Future, sync::Arc};
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

            $crate::generate_api!($authenticator | $api_config | $($object),*);
            $crate::generate_client!($authenticator | $api_config | $client_db | $($name: $object),*);
            $crate::generate_server!($authenticator | $api_config | $server_config | $($object),*);
        }
    }
}
