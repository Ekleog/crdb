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

struct ObjectId(Uuid);
struct EventId(Uuid);
struct TypeId(Uuid);

trait Db {
    fn set_new_object_cb(&mut self, cb: Box<dyn Fn(ObjectId, TypeId, serde_json::Value)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn Fn(ObjectId, EventId, TypeId, serde_json::Value)>);

    fn create(&self, object_id: ObjectId, object: serde_json::Value) -> anyhow::Result<()>;
    fn get(&self, ptr: ObjectId) -> anyhow::Result<serde_json::Value>;
    fn submit(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: serde_json::Value,
    ) -> anyhow::Result<()>;
    fn snapshot(&self, object: ObjectId) -> anyhow::Result<()>;
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
