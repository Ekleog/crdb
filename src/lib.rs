mod api;
pub use api::{DbPtr, Object, User};

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
    ( mod $module:ident auth $authenticator:ty { $($object:ty),* $(,)* } ) => {
        mod $module {
            $crate::client::generate_client!($authenticator / $($object),*);
            $crate::server::generate_server!($authenticator / $($object),*);
        }
    }
}
