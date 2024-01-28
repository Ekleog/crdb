use crate::{
    future::{CrdbSend, CrdbSync},
    BinPtr, CrdbFuture, EventId, Object, ObjectId, Updatedness,
};
use std::sync::Arc;

pub trait Db: 'static + CrdbSend + CrdbSync {
    /// Returns the new latest snapshot if it actually changed
    ///
    /// `updatedness` is the up-to-date-ness of this creation, or `None` if it is not known yet
    /// (eg. it was initiated client-side and has not round-tripped to server yet)
    fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        lock: bool,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<T>>>>;

    /// Returns the new latest snapshot if it actually changed
    ///
    /// `updatedness` is the up-to-date-ness of this submission, or `None` if it is not known yet
    /// (eg. it was initiated client-side and has not round-tripped to server yet)
    fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        force_lock: bool,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<T>>>>;

    fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> impl CrdbFuture<Output = crate::Result<Arc<T>>>;

    /// Either create an object if it did not exist yet, or recreate it
    ///
    /// Returns the new latest snapshot if it actually changed.
    ///
    /// `updatedness` is the up-to-date-ness of this recreation, or `None` if it is not known yet
    /// (eg. it was initiated client-side and has not round-tripped to server yet)
    fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        creation_value: Arc<T>,
        updatedness: Option<Updatedness>,
        force_lock: bool,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<T>>>>;

    fn remove(&self, object_id: ObjectId) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn create_binary(
        &self,
        binary_id: BinPtr,
        data: Arc<[u8]>,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn get_binary(
        &self,
        binary_id: BinPtr,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<[u8]>>>>;
}
