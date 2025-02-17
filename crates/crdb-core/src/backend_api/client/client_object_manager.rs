use std::sync::Arc;

use crate::{ClientSavedObjectMeta, DbPtr, EventId, Importance, Object, ObjectId};

pub trait ClientObjectManager: 'static + waaaa::Send + waaaa::Sync {
    // TODO(api-med): replace the Vec with a Stream
    fn list_saved_objects(
        &self,
    ) -> impl waaaa::Future<Output = crate::Result<Vec<ClientSavedObjectMeta>>>;

    /// Returns the new latest snapshot if it actually changed
    fn client_create<T: Object>(
        &self,
        object_id: DbPtr<T>,
        created_at: EventId,
        object: Arc<T>,
        importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    /// Returns the new latest snapshot if it actually changed
    fn client_submit<T: Object>(
        &self,
        object: DbPtr<T>,
        event_id: EventId,
        event: Arc<T::Event>,
        additional_importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<Option<Arc<T>>>>;

    fn set_object_importance(
        &self,
        object_id: ObjectId,
        new_importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn set_object_importance_from_queries(
        &self,
        object_id: ObjectId,
        new_importance_from_queries: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
