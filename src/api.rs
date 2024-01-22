use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    error::ResultExt,
    future::{CrdbSend, CrdbSync},
    BinPtr, CrdbFuture, EventId, ObjectId, Timestamp, TypeId, User,
};
use anyhow::Context;
use std::{any::Any, collections::HashSet, marker::PhantomData, sync::Arc};
use ulid::Ulid;

pub(crate) mod query;
pub use query::{JsonPathItem, Query};

pub(crate) mod private {
    pub trait Sealed {}
}

pub trait CanDoCallbacks: CrdbSend + CrdbSync + private::Sealed {
    fn get<T: Object>(&self, ptr: DbPtr<T>)
        -> impl '_ + CrdbFuture<Output = crate::Result<Arc<T>>>;
}

impl<D: Db> private::Sealed for D {}

impl<D: Db> CanDoCallbacks for D {
    async fn get<T: Object>(&self, object_id: DbPtr<T>) -> crate::Result<Arc<T>> {
        Ok(<D as Db>::get::<T>(&self, false, ObjectId(object_id.id))
            .await
            .wrap_with_context(|| format!("requesting {object_id:?} from database"))?
            .last_snapshot()
            .wrap_with_context(|| format!("retrieving last snapshot for {object_id:?}"))?)
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

    fn type_ulid() -> &'static TypeId;
    fn snapshot_version() -> i32 {
        0
    }
    /// Parse this object type from an older snapshot version
    ///
    /// Note that all metadata, in particular `required_binaries` and `users_who_can_read`
    /// MUST NOT change with a change in versioning. This method is designed only for
    /// changing the on-the-wire representation of an object, not for changing its semantics.
    ///
    /// Semantics changes should happen by sending an "upgrade" event to the object, and
    /// if cleanup is warranted then performing mass object recreation on the server afterwise.
    #[allow(unused_variables)]
    fn from_old_snapshot(version: i32, data: serde_json::Value) -> anyhow::Result<Self> {
        unimplemented!()
    }

    fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        self_id: ObjectId,
        db: &'a C,
    ) -> impl 'a + CrdbFuture<Output = anyhow::Result<bool>>;
    /// Note that permissions are always checked with the latest version of the object on the server.
    /// So, due to this, CRDB objects are not strictly speaking a CRDT. However, it is required to do
    /// so for security, because otherwise a user who lost permissions would still be allowed to
    /// submit events antidated to before the permission loss, which would be bad as users could
    /// re-grant themselves permissions.
    fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        self_id: ObjectId,
        event: &'a Self::Event,
        db: &'a C,
    ) -> impl 'a + CrdbFuture<Output = anyhow::Result<bool>>;
    /// Note that `db.get` calls will be cached. So:
    /// - Use `db.get` as little as possible, to avoid useless cache thrashing
    /// - Make sure to always read objects in a given order. You should consider all your objects as
    ///   forming a DAG, and each object's `users_who_can_read` function should:
    ///   - Only ever operate on a topological sort of the DAG
    ///   - Only call `db.get` on objects after this object on the topological sort
    ///   Failing to do this might lead to deadlocks within the database, which will result in internal
    ///   server errors from postgresql.
    ///   For example, if you have A -> B -> C and A -> C, A's `users_who_can_read` should first call
    ///   `get` on `B` before calling it on `C`, because otherwise B could be running the same function
    ///   on `C` and causing a deadlock.
    ///   Similarly, if A and B both depend on C and D, then `users_who_can_read` for A and B should
    ///   always lock C and D in the same order, to avoid deadlocks.
    ///   In other words, you should consider `db.get()` as taking a lock on the obtained object.
    fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> impl 'a + CrdbFuture<Output = anyhow::Result<Vec<User>>>;

    fn apply(&mut self, self_id: DbPtr<Self>, event: &Self::Event);

    // TODO(low): replace this boilerplate by some serialization dark magic to auto-detect all the fields?
    // This would be like-ish what we do for SearchableString, except we'd need some more thinking,
    // eg. a custom Serializer that'd collect only the _crdb-bin-ptr. Also we'd still need "regular"
    // serialization to be transparent, because users could query() on them.
    fn required_binaries(&self) -> Vec<BinPtr>;
}

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

#[cfg(target_arch = "wasm32")]
pub fn parse_snapshot_js<T: Object>(
    snapshot_version: i32,
    snapshot_data: wasm_bindgen::JsValue,
) -> crate::Result<T> {
    if snapshot_version == T::snapshot_version() {
        Ok(
            serde_wasm_bindgen::from_value(snapshot_data).wrap_with_context(|| {
                format!(
                    "parsing current snapshot version {snapshot_version} for object type {:?}",
                    T::type_ulid()
                )
            })?,
        )
    } else {
        let as_serde_json = serde_wasm_bindgen::from_value::<serde_json::Value>(snapshot_data)
            .wrap_with_context(|| format!("parsing data from IndexedDB as JSON"))?;
        T::from_old_snapshot(snapshot_version, as_serde_json).wrap_with_context(|| {
            format!(
                "parsing old snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })
    }
}

#[derive(Clone, Eq, PartialEq, educe::Educe, serde::Deserialize, serde::Serialize)]
#[educe(Debug(named_field = false), Ord, PartialOrd)]
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

    #[cfg(feature = "_tests")]
    pub fn from_string(s: &str) -> anyhow::Result<DbPtr<T>> {
        Ok(DbPtr {
            id: Ulid::from_string(s)?,
            _phantom: PhantomData,
        })
    }
}

#[cfg(feature = "_tests")]
impl<T: Object> bolero::TypeGenerator for DbPtr<T> {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<DbPtr<T>> {
        <[u8; 16]>::generate(driver).map(|b| Self {
            id: Ulid::from_bytes(b),
            _phantom: PhantomData,
        })
    }
}

#[allow(dead_code)] // TODO(api): remove
pub struct RequestId(Ulid);

#[allow(dead_code)] // TODO(api): remove
pub enum NewThing {
    Object(TypeId, ObjectId, serde_json::Value),
    Event(TypeId, ObjectId, EventId, serde_json::Value),
    Recreation(TypeId, ObjectId, Timestamp),
    Binary(BinPtr, Arc<Vec<u8>>),
    CurrentTime(Timestamp),
}

#[allow(dead_code)] // TODO(api): remove
pub enum Request {
    Subscribe(HashSet<ObjectId>),
    Unsubscribe(HashSet<ObjectId>),
    GetTime,
    Upload(Vec<UploadOrBinary>),
    // TODO(api): does this have everything we want to do?
}

#[allow(dead_code)] // TODO(api): remove
pub enum Upload {
    Object {
        object_id: ObjectId,
        type_id: TypeId,
        created_at: EventId,
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        type_id: TypeId,
        object_id: ObjectId,
        data: serde_json::Value,
    },
}

#[allow(dead_code)] // TODO(api): remove
pub enum UploadOrBinary {
    Upload(Upload),
    Binary(Arc<Vec<u8>>),
}

pub enum UploadOrBinPtr {
    Upload(Upload),
    BinPtr(BinPtr),
}

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UploadId(i64);

/// One ServerMessage is supposed to hold as many NewThings as possible
/// without delaying updates, but still avoiding going too far above
/// than 1M / message, to allow for better resumability.
///
/// As an exception, if `as_answer_to` is set, then `new_things` contains
/// at least the requested thing(s)
#[allow(dead_code)] // TODO(api): remove
pub struct ServerMessage {
    updates_on_server_until: Timestamp,
    as_answer_to: Option<RequestId>,
    new_things: Vec<NewThing>,
}

#[allow(dead_code)] // TODO(api): remove
pub struct ClientMessage {
    request_id: RequestId,
    request: Request,
}

pub trait ApiConfig: crate::private::Sealed {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Panics if there are two types with the same ULID configured
    fn check_ulids();

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `db`'s `create` method with the proper type and the fields from `o`.
    fn create<D: Db, C: CanDoCallbacks>(
        db: &D,
        o: DynNewObject,
        lock: bool,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `db`'s `submit` method with the proper type and the fields from `o`.
    fn submit<D: Db, C: CanDoCallbacks>(
        db: &D,
        e: DynNewEvent,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `db`'s `recreate` method with the proper type and the fields from `s`.
    fn recreate<D: Db, C: CanDoCallbacks>(
        db: &D,
        s: DynNewRecreation,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
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
                        panic!("Type ULID {u:?} was used multiple times!");
                    }
                }
            }

            async fn create<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, o: crdb::DynNewObject, lock: bool, cb: &C) -> crdb::Result<()> {
                $(
                    if o.type_id == *<$object as crdb::Object>::type_ulid() {
                        let object = o.object
                            .arc_to_any()
                            .downcast::<$object>()
                            .expect("got new object that could not be downcast to its type_id");
                        return db.create::<$object, _>(o.id, o.created_at, object, lock, cb).await;
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(o.type_id))
            }

            async fn submit<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, e: crdb::DynNewEvent, cb: &C) -> crdb::Result<()> {
                $(
                    if e.type_id == *<$object as crdb::Object>::type_ulid() {
                        let event = e.event
                            .arc_to_any()
                            .downcast::<<$object as crdb::Object>::Event>()
                            .expect("got new event that could not be downcast to its type_id");
                        return db.submit::<$object, _>(e.object_id, e.id, event, cb).await;
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(e.type_id))
            }

            async fn recreate<D: crdb::Db, C: crdb::CanDoCallbacks>(db: &D, s: crdb::DynNewRecreation, cb: &C) -> crdb::Result<()> {
                $(
                    if s.type_id == *<$object as crdb::Object>::type_ulid() {
                        return db.recreate::<$object, C>(s.time, s.object_id, cb).await;
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(s.type_id))
            }
        }
    };
}
