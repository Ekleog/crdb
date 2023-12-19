mod api;
pub use api::{DbPtr, Object, User};

#[cfg(feature = "client")]
pub mod client;
#[cfg(not(feature = "client"))]
pub mod client {
    #[macro_export]
    macro_rules! generate_client {
        ($($_:tt)*) => {}
    }
}

#[cfg(feature = "server")]
pub mod server;
#[cfg(not(feature = "client"))]
pub mod client {
    #[macro_export]
    macro_rules! generate_server {
        ($($_:tt)*) => {}
    }
}

#[macro_export]
macro_rules! db {
    ( $module:ident : $($object:ty),* $(,)* ) => {
        mod $module {
            $crate::client::generate_client!($($object),*);
            $crate::server::generate_server!($($object),*);
        }
    }
}
