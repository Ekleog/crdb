use std::sync::Arc;

use crate::{EventId, Object, ObjectId, Updatedness};

pub trait ServerObjectManager: 'static + waaaa::Send + waaaa::Sync {
    /// Returns the new latest snapshot if it actually changed
    fn server_recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        creation_value: Arc<T>,
        now_have_all_until: Updatedness,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    /// Returns the new latest snapshot if it actually changed
    fn server_submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        now_have_all_until: Updatedness,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    fn server_remove_object(
        &self,
        object_id: ObjectId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    // TODO(test-high): verify db fuzzers actually fuzz everything
    fn server_remove_event<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
