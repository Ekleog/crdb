mod api;
pub use api::{DbPtr, JsonPathItem, Object, Query, User};

mod cache;
mod traits;

#[cfg(feature = "client")]
pub mod client;
#[cfg(not(feature = "client"))]
pub mod client {
    #[macro_export]
    macro_rules! generate_client {
        ($($_:tt)*) => {};
    }
}

#[cfg(feature = "server")]
pub mod server;
#[cfg(not(feature = "server"))]
pub mod server {
    #[macro_export]
    macro_rules! generate_server {
        ($($_:tt)*) => {};
    }
}

#[macro_export]
macro_rules! db {
    (
        $v:vis mod $module:ident {
            auth: $authenticator:ty,
            server_config: $server_config:ident,
            client_db: $client_db:ident,
            objects: [ $($object:ty),* $(,)* ],
        }
    ) => {
        $v mod $module {
            $crate::generate_client!($authenticator | $client_db | $($object),*);
            $crate::generate_server!($authenticator | $server_config | $($object),*);
        }
    }
}
