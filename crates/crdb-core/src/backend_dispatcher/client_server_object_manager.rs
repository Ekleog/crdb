use crate::{backend_api, EventId, JsonSnapshot, ObjectId, TypeId, Updatedness};

use super::private;

pub trait ClientServerObjectManager: private::Sealed {
    /// The returned `JsonSnapshot` is guaranteed to be a serialization at the current snapshot
    /// version, of the latest snapshot of the object after the recreation.
    fn client_server_recreate<D: backend_api::client::ServerObjectManager>(
        call_on: &D,
        // type_id is inside JsonSnapshot
        object_id: ObjectId,
        new_created_at: EventId,
        creation_snapshot: JsonSnapshot,
        now_have_all_until: Updatedness,
    ) -> impl waaa::Future<Output = crate::Result<Option<JsonSnapshot>>>;

    /// The returned `JsonSnapshot` is guaranteed to be a serialization at the current snapshot
    /// version, of the latest snapshot of the object after the event submission.
    fn client_server_submit<D: backend_api::client::ServerObjectManager>(
        call_on: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: serde_json::Value,
        now_have_all_until: Updatedness,
    ) -> impl waaa::Future<Output = crate::Result<Option<JsonSnapshot>>>;

    // TODO(api-highest): why do we not actually need client_server_remove_object?
    /// The returned `JsonSnapshot` is guaranteed to be a serialization at the current snapshot
    /// version, of the latest snapshot of the object after the event removal.
    fn client_server_remove_event<D: backend_api::client::ServerObjectManager>(
        call_on: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
    ) -> impl waaa::Future<Output = crate::Result<Option<JsonSnapshot>>>;
}
