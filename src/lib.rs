use std::{any::Any, collections::BTreeMap, sync::Arc};
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ObjectId(Uuid);
struct EventId(Uuid);
struct TypeId(Uuid);

#[derive(Clone, Eq, PartialEq)]
enum MaybeParsed<T> {
    Json(Arc<serde_json::Value>),
    Parsed(Arc<T>),
}

#[derive(Clone)]
enum MaybeParsedAny {
    Json(Arc<serde_json::Value>),
    Parsed(Arc<dyn Any + Send + Sync>),
}

impl<T: Any + Send + Sync> From<MaybeParsed<T>> for MaybeParsedAny {
    fn from(value: MaybeParsed<T>) -> Self {
        match value {
            MaybeParsed::Json(v) => MaybeParsedAny::Json(v),
            MaybeParsed::Parsed(v) => MaybeParsedAny::Parsed(v),
        }
    }
}

impl MaybeParsedAny {
    fn downcast<T: Any + Send + Sync>(self) -> anyhow::Result<MaybeParsed<T>> {
        Ok(match self {
            MaybeParsedAny::Json(v) => MaybeParsed::Json(v),
            MaybeParsedAny::Parsed(v) => MaybeParsed::Parsed(
                v.downcast()
                    .map_err(|_| anyhow::anyhow!("Failed downcasting to expected type"))?,
            ),
        })
    }
}

struct CachedObject {
    creation_time: Timestamp,
    creation: MaybeParsedAny,
    last_snapshot_time: Timestamp,
    last_snapshot: MaybeParsedAny,
    events: BTreeMap<Timestamp, Vec<MaybeParsedAny>>,
}

#[derive(Clone, Copy)]
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
