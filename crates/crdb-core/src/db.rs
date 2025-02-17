use crate::{BinPtr, EventId, Importance, Object, ObjectId, Updatedness};
use std::sync::Arc;

// TODO(api-high): Review what's in this trait (and in Client/ServerSideDb), and verify that everything is necessary
// and, for Db, couldn't be moved to one of the side-specific traits.
pub trait Db: 'static + waaaa::Send + waaaa::Sync {
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
        additional_importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

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
        additional_importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    fn get_latest<T: Object>(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<Arc<T>>>;

    fn create_binary(
        &self,
        binary_id: BinPtr,
        data: Arc<[u8]>,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn get_binary(
        &self,
        binary_id: BinPtr,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<[u8]>>>>;

    /// Returns the number of errors that happened while re-encoding
    fn reencode_old_versions<T: Object>(&self) -> impl waaaa::Future<Output = usize>;

    fn assert_invariants_generic(&self) -> impl waaaa::Future<Output = ()>;
    fn assert_invariants_for<T: Object>(&self) -> impl waaaa::Future<Output = ()>;
}
