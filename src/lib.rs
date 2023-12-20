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

enum MaybeParsed<T: for<'a> serde::Deserialize<'a> + serde::Serialize> {
    Json(serde_json::Value),
    Parsed(T),
}

struct Timestamp(u64); // Nanoseconds since UNIX_EPOCH divided by 10

trait Db {
    fn set_new_object_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    );
    fn set_new_event_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
    );

    async fn create<T: Object>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        object: MaybeParsed<T>,
    ) -> anyhow::Result<()>;
    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<MaybeParsed<T>>;
    async fn submit<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: MaybeParsed<T::Event>,
    ) -> anyhow::Result<()>;
    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()>;
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
