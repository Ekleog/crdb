use crate::{BinPtr, Db, EventId, Lock, Object, ObjectId, Updatedness, Upload, UploadId};
use std::sync::Arc;

pub trait ClientSideDb: 'static + waaaa::Send + waaaa::Sync + Db {
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
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    fn remove(&self, object_id: ObjectId) -> impl waaaa::Future<Output = crate::Result<()>>;

    // TODO(test-high): introduce in db fuzzers
    fn remove_event<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn change_locks(
        &self,
        unlock: Lock,
        then_lock: Lock,
        object_id: ObjectId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn list_uploads(&self) -> impl waaaa::Future<Output = crate::Result<Vec<UploadId>>>;

    fn get_upload(
        &self,
        upload_id: UploadId,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Upload>>>;

    fn enqueue_upload(
        &self,
        upload: Upload,
        required_binaries: Vec<BinPtr>,
    ) -> impl waaaa::Future<Output = crate::Result<UploadId>>;

    fn upload_finished(
        &self,
        upload_id: UploadId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
