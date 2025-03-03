use std::sync::Arc;

use crate::{
    EventId, JsonSnapshot, Object, ObjectData, ObjectId, ReadPermsChanges, Updatedness, User,
};

pub trait ObjectManager {
    fn get_latest_json(
        &self,
        user: User,
        object_id: ObjectId,
    ) -> impl waaa::Future<Output = crate::Result<JsonSnapshot>>;

    // TODO(test-high): make sure fuzzers encompass all functions
    fn get_all_json(
        &self,
        user: User,
        object_id: ObjectId,
        only_updated_since: Option<Updatedness>,
    ) -> impl waaa::Future<Output = crate::Result<ObjectData>>;

    /// Returns the new latest snapshot in `Some` iff the object actually changed
    fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: &Arc<T>,
        updatedness: Updatedness,
    ) -> impl '_ + waaa::Future<Output = crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>>>;

    /// Returns the new latest snapshot in `Some` iff the object actually changed
    fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
    ) -> impl '_ + waaa::Future<Output = crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>>>;

    fn recreate_at<'a, T: Object>(
        &'a self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
    ) -> impl 'a + waaa::Future<Output = crate::Result<()>>;

    /// Update all the read rdeps that were still pending from the previous run
    fn update_pending_rdeps(&self) -> impl '_ + waaa::Future<Output = crate::Result<()>>;
}
