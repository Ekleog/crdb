use crate::{
    full_object::FullObject,
    future::{CrdbSend, CrdbSync},
    BinPtr, CanDoCallbacks, CrdbFuture, EventId, Object, ObjectId, Timestamp,
};
use std::sync::Arc;

pub trait Db: 'static + CrdbSend + CrdbSync {
    /// TODO(high): Returns the new latest snapshot if it actually changed
    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    /// TODO(high): Returns the new latest snapshot if it actually changed
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    // TODO(high): make into get_latest, only server needs actual get-full behavior
    fn get<T: Object>(
        &self,
        lock: bool,
        ptr: ObjectId,
    ) -> impl CrdbFuture<Output = crate::Result<FullObject>>;

    // TODO(high): remove from Db trait? it has no impact on the latest-snapshot cache anyway
    fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
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
