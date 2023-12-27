use crate::cache::CacheConfig;

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait Config: crate::private::Sealed {
    type Auth;

    type ApiConfig: CacheConfig;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_server {
    ( $auth:ty | $api_config:ident | $name:ident | $($object:ty),* ) => {
        pub struct $name;

        impl crdb::private::Sealed for $name {}
        impl crdb::ServerConfig for $name {
            type Auth = $auth;
            type ApiConfig = $api_config;
        }
    };
}
