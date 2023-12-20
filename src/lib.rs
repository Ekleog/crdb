use uuid::Uuid;

mod api;
pub use api::{DbPtr, Object, User};

mod cache;

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

trait Db {
    fn set_new_object_cb(&mut self, cb: Box<dyn FnMut(Uuid, serde_json::Value)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn FnMut(Uuid, Uuid, serde_json::Value)>);

    fn create(&self, object_id: Uuid, object: serde_json::Value) -> anyhow::Result<()>;
    fn get(&self, ptr: Uuid) -> anyhow::Result<serde_json::Value>;
    fn submit(&self, object: Uuid, event_id: Uuid, event: serde_json::Value) -> anyhow::Result<()>;
    fn snapshot(&self, object: Uuid) -> anyhow::Result<()>;
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
