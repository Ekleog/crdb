use crate::{
    BinPtr, ClientStorageInfo, CrdbSyncFn, Db, EventId, Importance, LoginInfo, Object, ObjectId,
    Query, QueryId, TypeId, Updatedness, Upload, UploadId,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub struct SavedObjectMeta {
    pub type_id: TypeId,
    pub have_all_until: Option<Updatedness>,
    pub importance: Importance,
}

#[derive(Clone)]
pub struct SavedQuery {
    pub query: Arc<Query>,
    pub type_id: TypeId,
    pub have_all_until: Option<Updatedness>,
    pub importance: Importance,
}

impl SavedQuery {
    pub fn now_have_all_until(&mut self, until: Updatedness) {
        self.have_all_until = std::cmp::max(self.have_all_until, Some(until));
    }
}

pub trait ClientSideDb: 'static + waaaa::Send + waaaa::Sync + Db {
    fn storage_info(&self) -> impl waaaa::Future<Output = crate::Result<ClientStorageInfo>>;

    fn save_login(&self, _info: LoginInfo) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn get_saved_login(&self) -> impl waaaa::Future<Output = crate::Result<Option<LoginInfo>>>;

    fn get_json(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<serde_json::Value>>;

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
        additional_importance: Importance,
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

    fn set_object_importance(
        &self,
        object_id: ObjectId,
        new_importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn set_importance_from_queries(
        &self,
        object_id: ObjectId,
        new_importance_from_queries: Importance,
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

    fn get_saved_objects(
        &self,
    ) -> impl waaaa::Future<Output = crate::Result<HashMap<ObjectId, SavedObjectMeta>>>;

    fn get_saved_queries(
        &self,
    ) -> impl waaaa::Future<Output = crate::Result<HashMap<QueryId, SavedQuery>>>;

    fn record_query(
        &self,
        query_id: QueryId,
        query: Arc<Query>,
        type_id: TypeId,
        importance: Importance,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn set_query_importance(
        &self,
        query_id: QueryId,
        importance: Importance,
        objects_matching_query: Vec<ObjectId>,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn forget_query(
        &self,
        query_id: QueryId,
        objects_matching_query: Vec<ObjectId>,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn update_queries(
        &self,
        queries: &HashSet<QueryId>,
        now_have_all_until: Updatedness,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
