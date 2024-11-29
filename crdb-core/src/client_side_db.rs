use crate::{
    BinPtr, ClientStorageInfo, CrdbSyncFn, Db, EventId, Lock, LoginInfo, Object, ObjectId, Query,
    QueryId, TypeId, Updatedness, Upload, UploadId,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

// TODO(misc-med): refactor these into proper structs
pub type SubscribedObject = (TypeId, serde_json::Value, Option<Updatedness>);
pub type SubscribedQuery = (Arc<Query>, TypeId, Option<Updatedness>, Lock);

pub trait ClientSideDb: 'static + waaaa::Send + waaaa::Sync + Db {
    fn storage_info(&self) -> impl waaaa::Future<Output = crate::Result<ClientStorageInfo>>;

    fn save_login(&self, _info: LoginInfo) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn get_saved_login(&self) -> impl waaaa::Future<Output = crate::Result<Option<LoginInfo>>>;

    fn remove_everything(&self) -> impl waaaa::Future<Output = crate::Result<()>>;

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

    fn client_query(
        &self,
        type_id: TypeId,
        query: Arc<Query>,
    ) -> impl waaaa::Future<Output = crate::Result<Vec<ObjectId>>>;

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

    fn client_vacuum(
        &self,
        notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
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

    fn get_subscribed_objects(
        &self,
    ) -> impl waaaa::Future<Output = crate::Result<HashMap<ObjectId, SubscribedObject>>>;

    fn get_subscribed_queries(
        &self,
    ) -> impl waaaa::Future<Output = crate::Result<HashMap<QueryId, SubscribedQuery>>>;

    fn subscribe_query(
        &self,
        _query_id: QueryId,
        _query: Arc<Query>,
        _type_id: TypeId,
        _lock: bool,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn unsubscribe_query(
        &self,
        _query_id: QueryId,
        _objects_to_unlock: Vec<ObjectId>,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn update_queries(
        &self,
        _queries: &HashSet<QueryId>,
        _now_have_all_until: Updatedness,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
