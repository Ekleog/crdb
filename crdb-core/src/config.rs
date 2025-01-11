use crate::{
    CanDoCallbacks, ClientSideDb, Db, EventId, Lock, ObjectId, ReadPermsChanges, ServerSideDb,
    TypeId, Update, Updatedness, User,
};
use std::{collections::HashSet, sync::Arc};

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UploadId(pub i64);

// Each update is both the list of updates itself, and the new latest snapshot
// for query matching, available if the latest snapshot actually changed. Also,
// the list of users allowed to read this object.
#[derive(Debug)]
pub struct UpdatesWithSnap {
    // The list of actual updates
    pub updates: Vec<Arc<Update>>,

    // The new last snapshot, if the update did change it (ie. no vacuum) and if the users affected
    // actually do have access to it. This is used for query matching.
    pub new_last_snapshot: Option<Arc<serde_json::Value>>,
}

// This module needs to actually be public, because the `db` macro needs to be
// able to implement it. However, making it doc(hidden) makes it look as though
// it is actually sealed in the documentation, which is good because we don't want
// users to rely on any stability guarantees there.
#[doc(hidden)]
pub mod private {
    pub trait Sealed {}
}

pub trait Config: 'static + Send + Sync + private::Sealed {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Panics if there are two types with the same ULID configured
    fn check_ulids();

    fn reencode_old_versions<D: Db>(call_on: &D) -> impl '_ + waaaa::Future<Output = usize>;

    fn create<D: Db>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        created_at: EventId,
        snapshot_version: i32,
        object: &serde_json::Value,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    /// The returned serde_json::Value is guaranteed to be a serialization of the latest snapshot of the object at the current snapshot version
    #[allow(clippy::too_many_arguments)] // Used only for relaying to a more specific function
    fn recreate<D: ClientSideDb>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        created_at: EventId,
        snapshot_version: i32,
        object: &serde_json::Value,
        updatedness: Option<Updatedness>,
        force_lock: Lock,
    ) -> impl waaaa::Future<Output = crate::Result<Option<serde_json::Value>>>;

    /// The returned serde_json::Value is guaranteed to be a serialization of the latest snapshot of the object at the current snapshot version
    fn submit<D: Db>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: &serde_json::Value,
        updatedness: Option<Updatedness>,
        force_lock: Lock,
    ) -> impl waaaa::Future<Output = crate::Result<Option<serde_json::Value>>>;

    fn remove_event<D: ClientSideDb>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn get_users_who_can_read<'a, D: ServerSideDb, C: CanDoCallbacks>(
        call_on: &'a D,
        object_id: ObjectId,
        type_id: TypeId,
        snapshot_version: i32,
        snapshot: serde_json::Value,
        cb: &'a C,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<(HashSet<User>, Vec<ObjectId>, Vec<D::Lock<'a>>)>>;

    #[allow(clippy::type_complexity)] // This is only used to proxy to a real function
    fn recreate_no_lock<'a, D: ServerSideDb, C: CanDoCallbacks>(
        call_on: &'a D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> impl 'a
           + waaaa::Future<
        Output = crate::Result<Option<(EventId, i32, serde_json::Value, HashSet<User>)>>,
    >;

    #[allow(clippy::too_many_arguments, clippy::type_complexity)] // This is only used to proxy to a real function
    fn upload_object<D: ServerSideDb>(
        call_on: &D,
        user: User,
        updatedness: Updatedness,
        type_id: TypeId,
        object_id: ObjectId,
        created_at: EventId,
        snapshot_version: i32,
        snapshot: Arc<serde_json::Value>,
    ) -> impl '_
           + waaaa::Future<
        Output = crate::Result<
            Option<(Arc<UpdatesWithSnap>, HashSet<User>, Vec<ReadPermsChanges>)>,
        >,
    >;

    /// The [`Vec<User>`] in return type is the list of users who can read the object both before and after the change. Users who gained or
    /// lost access to `object_id` are returned as part of the `Vec<ReadPermsChanges>`.
    #[allow(clippy::type_complexity)] // This is only used to proxy to a real function
    fn upload_event<D: ServerSideDb>(
        call_on: &D,
        user: User,
        updatedness: Updatedness,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<serde_json::Value>,
    ) -> impl '_
           + waaaa::Future<
        Output = crate::Result<Option<(Arc<UpdatesWithSnap>, Vec<User>, Vec<ReadPermsChanges>)>>,
    >;
}
