use crate::{
    backend_api, EventId, JsonSnapshot, ObjectId, ServerObjectUpdate, TypeId, Updatedness,
};

use super::private;

pub trait ServerObjectManager: private::Sealed {
    /// Recreate at a given `event_id`. Guaranteed to not affect the latest snapshot.
    fn server_recreate_at<'a, D: backend_api::server::ObjectManager>(
        call_on: &'a D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<()>>;

    /// Returns all the updates to send if the state changed.
    fn server_create<D: backend_api::server::ObjectManager>(
        call_on: &D,
        // type_id: part of JsonSnapshot
        object_id: ObjectId,
        created_at: EventId,
        object: JsonSnapshot,
        updatedness: Updatedness,
    ) -> impl '_ + waaaa::Future<Output = crate::Result<Option<ServerObjectUpdate>>>;

    /// Returns all the updates to send if the state changed.
    fn server_submit<D: backend_api::server::ObjectManager>(
        call_on: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: serde_json::Value,
        updatedness: Updatedness,
    ) -> impl '_ + waaaa::Future<Output = crate::Result<Option<ServerObjectUpdate>>>;
}
