use crate::{BinPtr, CrdbFuture, CrdbSend, CrdbSync, Db, DbPtr, ObjectId, ResultExt, TypeId, User};
use anyhow::Context;
use std::{any::Any, collections::HashSet, sync::Arc};

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
        self.get_latest::<T>(false, ObjectId(object_id.id))
            .await
            .wrap_with_context(|| format!("requesting {object_id:?} from database"))
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
    ) -> impl 'a + CrdbFuture<Output = anyhow::Result<HashSet<User>>>;

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

#[cfg(feature = "client")]
pub fn parse_snapshot_ref<T: Object>(
    snapshot_version: i32,
    snapshot_data: &serde_json::Value,
) -> anyhow::Result<T> {
    if snapshot_version == T::snapshot_version() {
        Ok(T::deserialize(snapshot_data).with_context(|| {
            format!(
                "parsing current snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })?)
    } else {
        T::from_old_snapshot(snapshot_version, snapshot_data.clone()).with_context(|| {
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
