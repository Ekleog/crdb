use crate::{
    cache::CacheConfig,
    db_trait::{Db, EventId, ObjectId, TypeId},
    Timestamp,
};
use anyhow::Context;
use std::{any::Any, collections::HashSet, future::Future, marker::PhantomData, sync::Arc};
use ulid::Ulid;

pub(crate) mod query;
pub use query::{JsonNumber, JsonPathItem, Query};

macro_rules! impl_for_id {
    ($type:ty) => {
        #[cfg(feature = "server")]
        impl $type {
            pub(crate) fn to_uuid(&self) -> uuid::Uuid {
                uuid::Uuid::from_bytes(self.id.to_bytes())
            }
        }

        #[cfg(feature = "server")]
        impl<'q> sqlx::encode::Encode<'q, sqlx::Postgres> for $type {
            fn encode_by_ref(
                &self,
                buf: &mut sqlx::postgres::PgArgumentBuffer,
            ) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode_by_ref(
                    &self.to_uuid(),
                    buf,
                )
            }
            fn encode(self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode(
                    self.to_uuid(),
                    buf,
                )
            }
            fn produces(&self) -> Option<sqlx::postgres::PgTypeInfo> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::produces(&self.to_uuid())
            }
            fn size_hint(&self) -> usize {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::size_hint(&self.to_uuid())
            }
        }

        #[cfg(feature = "server")]
        impl sqlx::Type<sqlx::Postgres> for $type {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "server")]
        impl sqlx::postgres::PgHasArrayType for $type {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
            fn array_compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_compatible(ty)
            }
        }

        #[cfg(test)]
        impl bolero::TypeGenerator for $type {
            fn generate<D: bolero::Driver>(driver: &mut D) -> Option<$type> {
                <[u8; 16]>::generate(driver).map(|b| Self {
                    id: Ulid::from_bytes(b),
                })
            }
        }

        // No allocations in the Ulid-only-containing types
        deepsize::known_deep_size!(0; $type);
    };
}

#[derive(Clone, Copy, Eq, PartialEq, educe::Educe, serde::Deserialize, serde::Serialize)]
#[educe(Debug)]
pub struct User {
    #[educe(Debug(method(std::fmt::Display::fmt)))]
    pub id: Ulid,
}

impl_for_id!(User);

mod private {
    pub trait Sealed {}
}

pub trait CanDoCallbacks: Send + Sync + private::Sealed {
    fn get<T: Object>(
        &self,
        ptr: DbPtr<T>,
    ) -> impl '_ + Send + Future<Output = anyhow::Result<Option<Arc<T>>>>;
}

impl<D: Db> private::Sealed for D {}

impl<D: Db> CanDoCallbacks for D {
    async fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Option<Arc<T>>> {
        Ok(<D as Db>::get::<T>(&self, ObjectId(ptr.id))
            .await
            .with_context(|| format!("requesting {ptr:?} from database"))?
            .map(|o| o.last_snapshot())
            .transpose()
            .with_context(|| format!("retrieving last snapshot for {ptr:?}"))?)
    }
}

pub trait Event:
    Any + Eq + Send + Sync + deepsize::DeepSizeOf + for<'a> serde::Deserialize<'a> + serde::Serialize
{
    fn required_binaries(&self) -> Vec<BinPtr>;
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
    + deepsize::DeepSizeOf
    + for<'a> serde::Deserialize<'a>
    + serde::Serialize
{
    /// Note that due to postgresql limitations reasons, this type MUST NOT include any
    /// null byte in the serialized JSON. Trying to submit one such event will result
    /// in the event being rejected by the server.
    type Event: Event;

    fn type_ulid() -> &'static Ulid;
    fn snapshot_version() -> i32 {
        0
    }
    #[allow(unused_variables)]
    fn from_old_snapshot(version: i32, data: serde_json::Value) -> anyhow::Result<Self> {
        unimplemented!()
    }
    // TODO: allow re-encoding all snapshots in db with the new version using from_old_snapshot

    fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        db: &'a C,
    ) -> impl 'a + Send + Future<Output = anyhow::Result<bool>>;
    /// Note that permissions are always checked with the latest version of the object on the server.
    /// So, due to this, CRDB objects are not strictly speaking a CRDT. However, it is required to do
    /// so for security, because otherwise a user who lost permissions would still be allowed to
    /// submit events antidated to before the permission loss, which would be bad as users could
    /// re-grant themselves permissions.
    fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        event: &'a Self::Event,
        db: &'a C,
    ) -> impl 'a + Send + Future<Output = anyhow::Result<bool>>;
    fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> impl 'a + Send + Future<Output = anyhow::Result<Vec<User>>>;

    fn apply(&mut self, event: &Self::Event);

    fn is_heavy(&self) -> bool;
    fn required_binaries(&self) -> Vec<BinPtr>;
}

#[cfg(feature = "server")]
pub fn parse_snapshot<T: Object>(
    snapshot_version: i32,
    snapshot_data: serde_json::Value,
) -> anyhow::Result<T> {
    if snapshot_version == T::snapshot_version() {
        Ok(serde_json::from_value(snapshot_data).with_context(|| {
            format!(
                "parsing current snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })?)
    } else {
        T::from_old_snapshot(snapshot_version, snapshot_data).with_context(|| {
            format!(
                "parsing old snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })
    }
}

#[derive(Clone, Eq, PartialEq, educe::Educe, serde::Deserialize, serde::Serialize)]
#[educe(Debug(named_field = false))]
pub struct DbPtr<T: Object> {
    #[educe(Debug(method = std::fmt::Display::fmt))]
    pub id: Ulid,
    #[educe(Debug(ignore))]
    _phantom: PhantomData<T>,
}

impl<T: Object> deepsize::DeepSizeOf for DbPtr<T> {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl<T: Object> Copy for DbPtr<T> {}

impl<T: Object> DbPtr<T> {
    pub fn from(id: ObjectId) -> DbPtr<T> {
        DbPtr {
            id: id.0,
            _phantom: PhantomData,
        }
    }

    pub fn to_object_id(&self) -> ObjectId {
        ObjectId(self.id)
    }
}

#[cfg(test)]
impl<T: Object> bolero::TypeGenerator for DbPtr<T> {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<DbPtr<T>> {
        <[u8; 16]>::generate(driver).map(|b| Self {
            id: Ulid::from_bytes(b),
            _phantom: PhantomData,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BinPtr {
    pub(crate) id: Ulid,
}

impl_for_id!(BinPtr);

#[allow(dead_code)] // TODO: remove
pub struct RequestId(Ulid);

#[allow(dead_code)] // TODO: remove
pub enum NewThing {
    Object(TypeId, ObjectId, serde_json::Value),
    Event(TypeId, ObjectId, EventId, serde_json::Value),
    Recreation(TypeId, ObjectId, Timestamp),
    Binary(BinPtr, Vec<u8>),
    CurrentTime(Timestamp),
}

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
#[allow(dead_code)] // TODO: remove
pub struct ServerMessage {
    updates_on_server_until: Timestamp,
    as_answer_to: Option<RequestId>,
    new_things: Vec<NewThing>,
}

#[allow(dead_code)] // TODO: remove
pub struct ClientMessage {
    request_id: RequestId,
    request: Request,
}

pub trait Config: crate::private::Sealed + CacheConfig {
    /// Panics if there are two types with the same ULID configured
    fn check_ulids();
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_api {
    ( $authenticator:ty | $config:ident | $($object:ty),* ) => {
        pub struct $config;

        impl crdb::private::Sealed for $config {}
        impl crdb::ApiConfig for $config {
            fn check_ulids() {
                let ulids = [$(<$object as crdb::Object>::type_ulid()),*];
                for u in ulids.iter() {
                    if ulids.iter().filter(|i| *i == u).count() != 1 {
                        panic!("Type ULID {u} was used multiple times!");
                    }
                }
            }
        }

        impl crdb::CacheConfig for $config {
            async fn create(cache: &mut crdb::ObjectCache, o: crdb::DynNewObject) -> crdb::anyhow::Result<bool> {
                $(
                    if o.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        let object = o.object
                            .arc_to_any()
                            .downcast::<$object>()
                            .expect("got new object that could not be downcast to its type_id");
                        return cache.create::<$object>(o.id, o.created_at, object);
                    }
                )*
                crdb::anyhow::bail!("got new object with unknown type {:?}", o.type_id)
            }

            async fn submit(cache: &mut crdb::ObjectCache, e: crdb::DynNewEvent) -> crdb::anyhow::Result<bool> {
                $(
                    if e.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        let event = e.event
                            .arc_to_any()
                            .downcast::<<$object as crdb::Object>::Event>()
                            .expect("got new event that could not be downcast to its type_id");
                        return cache.submit::<$object>(e.object_id, e.id, event);
                    }
                )*
                crdb::anyhow::bail!("got new event with unknown type {:?}", e.type_id)
            }

            async fn recreate(cache: &mut crdb::ObjectCache, s: crdb::DynNewRecreation) -> crdb::anyhow::Result<()> {
                $(
                    if s.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        return cache.recreate::<$object>(s.object_id, s.time);
                    }
                )*
                crdb::anyhow::bail!("got new re-creation with unknown type {:?}", s.type_id)
            }

            async fn create_in_db<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, o: crdb::DynNewObject, cb: &C) -> Result<(), crdb::DbOpError> {
                $(
                    if o.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        let object = o.object
                            .arc_to_any()
                            .downcast::<$object>()
                            .expect("got new object that could not be downcast to its type_id");
                        return db.create::<$object, _>(o.id, o.created_at, object, cb).await;
                    }
                )*
                Err(crdb::DbOpError::Other(crdb::anyhow::anyhow!("got new object with unknown type {:?}", o.type_id)))
            }

            async fn submit_in_db<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, e: crdb::DynNewEvent, cb: &C) -> Result<(), crdb::DbOpError> {
                $(
                    if e.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        let event = e.event
                            .arc_to_any()
                            .downcast::<<$object as crdb::Object>::Event>()
                            .expect("got new event that could not be downcast to its type_id");
                        return db.submit::<$object, _>(e.object_id, e.id, event, cb).await;
                    }
                )*
                Err(crdb::DbOpError::Other(crdb::anyhow::anyhow!("got new event with unknown type {:?}", e.type_id)))
            }

            async fn recreate_in_db<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, s: crdb::DynNewRecreation, cb: &C) -> crdb::anyhow::Result<()> {
                $(
                    if s.type_id.0 == *<$object as crdb::Object>::type_ulid() {
                        return db.recreate::<$object, C>(s.time, s.object_id, cb).await;
                    }
                )*
                crdb::anyhow::bail!("got new re-creation with unknown type {:?}", s.type_id)
            }
        }
    };
}
