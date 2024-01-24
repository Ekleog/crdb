use crate::{
    full_object::FullObject,
    future::{CrdbSend, CrdbSync},
    BinPtr, CanDoCallbacks, CrdbFuture, CrdbStream, EventId, Object, ObjectId, Query, Timestamp,
    User,
};
use std::sync::Arc;

pub trait Db: 'static + CrdbSend + CrdbSync {
    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn get<T: Object>(
        &self,
        lock: bool,
        ptr: ObjectId,
    ) -> impl CrdbFuture<Output = crate::Result<FullObject>>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: &Query,
    ) -> impl CrdbFuture<Output = crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>>>;

    fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn unlock(&self, object_id: ObjectId) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn remove(&self, object_id: ObjectId) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn create_binary(
        &self,
        binary_id: BinPtr,
        data: Arc<[u8]>,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn get_binary(
        &self,
        binary_id: BinPtr,
    ) -> impl CrdbFuture<Output = anyhow::Result<Option<Arc<[u8]>>>>;
}
