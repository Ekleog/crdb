use crate::{
    future::{CrdbSend, CrdbSync},
    BinPtr, CanDoCallbacks, CrdbFuture, EventId, Object, ObjectId, Timestamp,
};
use std::sync::Arc;

pub trait Db: 'static + CrdbSend + CrdbSync {
    /// Returns the new latest snapshot if it actually changed
    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<T>>>>;

    /// TODO(high): Returns the new latest snapshot if it actually changed
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> impl CrdbFuture<Output = crate::Result<Arc<T>>>;

    fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        time: Timestamp,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

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
