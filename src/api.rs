use crate::{
    cache::CacheConfig,
    db_trait::{EventId, ObjectId, TypeId},
    Timestamp,
};
use std::{any::Any, collections::HashSet, marker::PhantomData, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy)]
pub struct User {
    pub id: Ulid,
}

#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),
    Id(usize),
}

#[non_exhaustive]
pub enum JsonNumber {
    F64(f64),
    I64(i64),
    U64(u64),
}

#[non_exhaustive]
pub enum Query {
    // Logic operators
    All(Vec<Query>),
    Any(Vec<Query>),
    Not(Box<Query>),

    // Any/all the values in the array at JsonPathItem must match Query
    AnyIn(Vec<JsonPathItem>, Box<Query>),
    AllIn(Vec<JsonPathItem>, Box<Query>),

    // JSON tests
    Eq(Vec<JsonPathItem>, serde_json::Value),
    Ne(Vec<JsonPathItem>, serde_json::Value),

    // Integers
    Le(Vec<JsonPathItem>, JsonNumber),
    Lt(Vec<JsonPathItem>, JsonNumber),
    Ge(Vec<JsonPathItem>, JsonNumber),
    Gt(Vec<JsonPathItem>, JsonNumber),

    // Arrays and object subscripting
    Contains(Vec<JsonPathItem>, serde_json::Value),

    // Full text search
    ContainsStr(Vec<JsonPathItem>, String),
}

mod private {
    pub trait Sealed {}
}

pub trait CanDoCallbacks: private::Sealed {
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
}

/// Note that due to postgresql limitations reasons, this type MUST NOT include any
/// null byte in the serialized JSON. Including them will result in internal server
/// errors.
pub trait Object:
    Any
    + Clone
    + Eq
    + Send
    + Sync
    + for<'a> serde::Deserialize<'a>
    + serde::Serialize
{
    /// Note that due to postgresql limitations reasons, this type MUST NOT include any
    /// null byte in the serialized JSON. Trying to submit one such event will result
    /// in the event being rejected by the server.
    type Event: Any
        + Eq
        + Send
        + Sync
        + for<'a> serde::Deserialize<'a>
        + serde::Serialize;

    fn ulid() -> &'static Ulid;
    fn snapshot_version() -> u64 {
        0
    }
    #[allow(unused_variables)]
    fn from_old_snapshot(version: u64, data: serde_json::Value) -> Self {
        unimplemented!()
    }

    fn can_create<C: CanDoCallbacks>(&self, user: User, db: &C) -> anyhow::Result<bool>;
    /// Note that permissions are always checked with the latest version of the object on the server.
    /// So, due to this, CRDB objects are not strictly speaking a CRDT. However, it is required to do
    /// so for security, because otherwise a user who lost permissions would still be allowed to
    /// submit events antidated to before the permission loss, which would be bad as users could
    /// re-grant themselves permissions.
    fn can_apply<C: CanDoCallbacks>(
        &self,
        user: &User,
        event: &Self::Event,
        db: &C,
    ) -> anyhow::Result<bool>;
    fn users_who_can_read<C: CanDoCallbacks>(&self) -> anyhow::Result<Vec<User>>;
    fn apply(&mut self, event: &Self::Event);

    fn is_heavy(&self) -> anyhow::Result<bool>;
    fn required_binaries(&self) -> Vec<BinPtr>;
}

pub struct DbPtr<T: Object> {
    #[doc(hidden)]
    pub id: Ulid,
    _phantom: PhantomData<T>,
}

impl<T: Object> DbPtr<T> {
    #[doc(hidden)]
    pub fn from(id: ObjectId) -> DbPtr<T> {
        DbPtr {
            id: id.0,
            _phantom: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn to_object_id(&self) -> ObjectId {
        ObjectId(self.id)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BinPtr {
    pub(crate) id: Ulid,
}

#[doc(hidden)]
#[allow(dead_code)] // TODO: remove
pub struct RequestId(Ulid);

#[doc(hidden)]
#[allow(dead_code)] // TODO: remove
pub enum NewThing {
    Object(TypeId, ObjectId, serde_json::Value),
    Event(TypeId, ObjectId, EventId, serde_json::Value),
    Snapshot(TypeId, ObjectId, Timestamp),
    Binary(BinPtr, Vec<u8>),
    CurrentTime(Timestamp),
}

#[doc(hidden)]
#[allow(dead_code)] // TODO: remove
pub enum Request {
    Subscribe(HashSet<ObjectId>),
    Unsubscribe(HashSet<ObjectId>),
    GetTime,
    // TODO
}

/// One ServerMessage is supposed to hold as many NewThings as possible
/// without delaying updates, but still avoiding going too far above
/// than 1M / message, to allow for better resumability.
#[doc(hidden)]
#[allow(dead_code)] // TODO: remove
pub struct ServerMessage {
    updates_on_server_until: Timestamp,
    as_answer_to: Option<RequestId>,
    new_things: Vec<NewThing>,
}

#[doc(hidden)]
#[allow(dead_code)] // TODO: remove
pub struct ClientMessage {
    request_id: RequestId,
    request: Request,
}

pub trait Config: crate::private::Sealed + CacheConfig {}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_api {
    ( $authenticator:ty | $config:ident | $($object:ty),* ) => {
        pub struct $config;

        impl crdb::private::Sealed for $config {}
        impl crdb::ApiConfig for $config {}

        impl crdb::CacheConfig for $config {
            fn create(cache: &mut crdb::ObjectCache, o: crdb::NewObject) -> crdb::anyhow::Result<bool> {
                $(
                    if o.type_id.0 == *<$object as crdb::Object>::ulid() {
                        let object = o.object
                            .downcast::<$object>()
                            .expect("got new object that could not be downcast to its type_id");
                        return cache.create::<$object>(o.id, o.created_at, object);
                    }
                )*
                crdb::anyhow::bail!("got new object with unknown type {:?}", o.type_id)
            }

            async fn submit<D: crdb::Db>(db: Option<&D>, cache: &mut crdb::ObjectCache, e: crdb::NewEvent) -> crdb::anyhow::Result<bool> {
                $(
                    if e.type_id.0 == *<$object as crdb::Object>::ulid() {
                        let event = e.event
                            .downcast::<<$object as crdb::Object>::Event>()
                            .expect("got new event that could not be downcast to its type_id");
                        return cache.submit::<D, $object>(db, e.object_id, e.id, event).await;
                    }
                )*
                crdb::anyhow::bail!("got new event with unknown type {:?}", e.type_id)
            }

            async fn snapshot(cache: &mut crdb::ObjectCache, s: crdb::NewSnapshot) -> crdb::anyhow::Result<()> {
                $(
                    if s.type_id.0 == *<$object as crdb::Object>::ulid() {
                        return cache.snapshot::<$object>(s.object_id, s.time).await;
                    }
                )*
                crdb::anyhow::bail!("got new snapshot with unknown type {:?}", s.type_id)
            }
        }
    };
}
