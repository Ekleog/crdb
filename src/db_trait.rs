use crate::{
    future::{CrdbSend, CrdbSync},
    BinPtr, CrdbFuture, EventId, Object, ObjectId, Updatedness,
};
use std::sync::Arc;

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct Lock: u8 {
        const NONE = 0;
        const OBJECT = 0b01;
        const FOR_QUERIES = 0b10;
    }
}

impl Lock {
    #[allow(dead_code)] // TODO(sqlite): remove once sqlite makes use of this too
    pub(crate) fn from_query_lock(b: bool) -> Lock {
        match b {
            true => Lock::FOR_QUERIES,
            false => Lock::NONE,
        }
    }
}

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
        lock: Lock,
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
        force_lock: Lock,
    ) -> impl CrdbFuture<Output = crate::Result<Option<Arc<T>>>>;

    fn get_latest<T: Object>(
        &self,
        lock: Lock,
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
        force_lock: Lock,
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
