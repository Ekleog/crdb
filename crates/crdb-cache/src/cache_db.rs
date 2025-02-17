use super::{BinariesCache, ObjectCache};
use crdb_core::{
    BinPtr, ClientSideDb, ClientStorageInfo, CrdbSyncFn, Db, DynSized, EventId, Importance,
    LoginInfo, Object, ObjectId, Query, QueryId, SavedObjectMeta, SavedQuery, ServerSideDb,
    Session, SessionRef, SessionToken, TypeId, Updatedness, Upload, UploadId, User,
    UsersWhoCanRead,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
};
use web_time::SystemTime;

pub struct CacheDb<D: Db> {
    db: D,
    cache: Arc<RwLock<ObjectCache>>,
    binaries: Arc<RwLock<BinariesCache>>,
}

impl<D: Db> CacheDb<D> {
    /// Note that `watermark` will still keep in the cache all objects still in use by the program.
    /// So, setting `watermark` to a value too low (eg. less than the size of objects actually in use by the
    /// program) would make cache operation slow.
    /// For this reason, if the cache used size reaches the watermark, then the watermark will be
    /// automatically increased.
    pub fn new(db: D, watermark: usize) -> CacheDb<D> {
        let cache = Arc::new(RwLock::new(ObjectCache::new(watermark)));
        CacheDb {
            db,
            cache,
            binaries: Arc::new(RwLock::new(BinariesCache::new())),
        }
    }

    // TODO(perf-high): auto-clear binaries without waiting for a reduce_size_to call
    // Probably both cache & binaries should be dealt with together by moving the
    // watermark handling to the CacheDb level. OTOH binaries are already Weak so
    // it's not a big deal
}

impl<D: Db> Db for CacheDb<D> {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        self.cache.write().unwrap().remove(&object_id);
        let res = self
            .db
            .create(
                object_id,
                created_at,
                object.clone(),
                updatedness,
                importance,
            )
            .await?;
        if let Some(value) = res.clone() {
            self.cache.write().unwrap().set(object_id, value);
        }
        Ok(res)
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .submit::<T>(
                object_id,
                event_id,
                event.clone(),
                updatedness,
                additional_importance,
            )
            .await?;
        if let Some(value) = res.clone() {
            self.cache.write().unwrap().set(object_id, value);
        }
        Ok(res)
    }

    async fn get_latest<T: Object>(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> crate::Result<Arc<T>> {
        if let Some(res) = self.cache.read().unwrap().get(&object_id) {
            // TODO(api-highest): if we need to increase the importance, this will skip it
            if let Ok(res) = Arc::downcast(DynSized::arc_to_any(res)) {
                return Ok(res);
            }
            // Ignore wrong type errors, they'll be catched by self.db.get_latest below, where the real type will be known
        }
        let res = self.db.get_latest::<T>(object_id, importance).await?;
        self.cache.write().unwrap().set(object_id, res.clone() as _);
        Ok(res)
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        // TODO(perf-med): this could be disabled on release builds, because downstream will re-hash anyway.
        // Actually, most of the hashes in create_binary could likely be ignored, except for the one at the
        // entrance of postgres.
        if crdb_core::hash_binary(&data) != binary_id {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        self.binaries
            .write()
            .unwrap()
            .insert(binary_id, Arc::downgrade(&data));
        self.db.create_binary(binary_id, data).await
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        if let Some(res) = self.binaries.read().unwrap().get(&binary_id) {
            return Ok(Some(res.clone()));
        }
        let Some(res) = self.db.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.binaries
            .write()
            .unwrap()
            .insert(binary_id, Arc::downgrade(&res));
        Ok(Some(res))
    }

    /// Returns the number of errors that happened while re-encoding
    async fn reencode_old_versions<T: Object>(&self) -> usize {
        self.db.reencode_old_versions::<T>().await
    }

    async fn assert_invariants_generic(&self) {
        self.db.assert_invariants_generic().await;
    }

    async fn assert_invariants_for<T: Object>(&self) {
        self.db.assert_invariants_for::<T>().await;
    }
}

impl<D: ClientSideDb> ClientSideDb for CacheDb<D> {
    async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        self.db.storage_info().await
    }

    async fn save_login(&self, info: LoginInfo) -> crate::Result<()> {
        self.db.save_login(info).await
    }

    async fn get_saved_login(&self) -> crate::Result<Option<LoginInfo>> {
        self.db.get_saved_login().await
    }

    async fn remove_everything(&self) -> crate::Result<()> {
        self.db.remove_everything().await
    }

    async fn get_json(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> crate::Result<serde_json::Value> {
        self.db.get_json(object_id, importance).await
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .recreate(
                object_id,
                new_created_at,
                object,
                updatedness,
                additional_importance,
            )
            .await?;
        if let Some(res) = res.clone() {
            self.cache.write().unwrap().set(object_id, res as _);
        }
        Ok(res)
    }

    async fn client_query(
        &self,
        type_id: TypeId,
        query: Arc<Query>,
    ) -> crate::Result<Vec<ObjectId>> {
        self.db.client_query(type_id, query).await
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        self.cache.write().unwrap().remove(&object_id);
        self.db.remove(object_id).await
    }

    async fn remove_event<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
    ) -> crate::Result<()> {
        self.cache.write().unwrap().remove(&object_id);
        self.db.remove_event::<T>(object_id, event_id).await
    }

    async fn set_object_importance(
        &self,
        object_id: ObjectId,
        new_importance: Importance,
    ) -> crate::Result<()> {
        self.db
            .set_object_importance(object_id, new_importance)
            .await
    }

    async fn set_importance_from_queries(
        &self,
        object_id: ObjectId,
        new_importance_from_queries: Importance,
    ) -> crate::Result<()> {
        self.db
            .set_importance_from_queries(object_id, new_importance_from_queries)
            .await
    }

    async fn client_vacuum(
        &self,
        notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
    ) -> crate::Result<()> {
        let objects_to_remove = Arc::new(Mutex::new(HashSet::new()));
        let res = self
            .db
            .client_vacuum(
                {
                    let objects_to_remove = objects_to_remove.clone();
                    move |object_id| {
                        objects_to_remove.lock().unwrap().insert(object_id);
                        notify_removals(object_id);
                    }
                },
                notify_query_removals,
            )
            .await;
        let mut cache = self.cache.write().unwrap();
        for o in objects_to_remove.lock().unwrap().iter() {
            cache.remove(o);
        }
        res
    }

    async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        self.db.list_uploads().await
    }

    async fn get_upload(&self, upload_id: UploadId) -> crate::Result<Option<Upload>> {
        self.db.get_upload(upload_id).await
    }

    async fn enqueue_upload(
        &self,
        upload: Upload,
        required_binaries: Vec<BinPtr>,
    ) -> crate::Result<UploadId> {
        self.db.enqueue_upload(upload, required_binaries).await
    }

    async fn upload_finished(&self, upload_id: UploadId) -> crate::Result<()> {
        self.db.upload_finished(upload_id).await
    }

    async fn get_saved_objects(&self) -> crate::Result<HashMap<ObjectId, SavedObjectMeta>> {
        self.db.get_saved_objects().await
    }

    async fn get_saved_queries(&self) -> crate::Result<HashMap<QueryId, SavedQuery>> {
        self.db.get_saved_queries().await
    }

    async fn record_query(
        &self,
        query_id: QueryId,
        query: Arc<Query>,
        type_id: TypeId,
        importance: Importance,
    ) -> crate::Result<()> {
        self.db
            .record_query(query_id, query, type_id, importance)
            .await
    }

    async fn set_query_importance(
        &self,
        query_id: QueryId,
        importance: Importance,
        objects_matching_query: Vec<ObjectId>,
    ) -> crate::Result<()> {
        self.db
            .set_query_importance(query_id, importance, objects_matching_query)
            .await
    }

    async fn forget_query(
        &self,
        query_id: QueryId,
        objects_matching_query: Vec<ObjectId>,
    ) -> crate::Result<()> {
        self.db.forget_query(query_id, objects_matching_query).await
    }

    async fn update_queries(
        &self,
        queries: &HashSet<QueryId>,
        now_have_all_until: Updatedness,
    ) -> crate::Result<()> {
        self.db.update_queries(queries, now_have_all_until).await
    }
}

impl<D: ServerSideDb> ServerSideDb for CacheDb<D> {
    type Connection = D::Connection;
    type Transaction<'a> = D::Transaction<'a>;
    type Lock<'a> = D::Lock<'a>;

    fn get_users_who_can_read<'a, 'ret: 'a, T: Object, C: crdb_core::CanDoCallbacks>(
        &'ret self,
        object_id: ObjectId,
        object: &'a T,
        cb: &'a C,
    ) -> std::pin::Pin<
        Box<dyn 'a + waaaa::Future<Output = anyhow::Result<UsersWhoCanRead<Self::Lock<'ret>>>>>,
    > {
        self.db.get_users_who_can_read(object_id, object, cb)
    }

    async fn get_transaction(&self) -> crdb_core::Result<Self::Transaction<'_>> {
        self.db.get_transaction().await
    }

    async fn get_latest_snapshot(
        &self,
        transaction: &mut Self::Connection,
        user: User,
        object_id: ObjectId,
    ) -> crate::Result<Arc<serde_json::Value>> {
        self.db
            .get_latest_snapshot(transaction, user, object_id)
            .await
    }

    async fn get_all(
        &self,
        connection: &mut Self::Connection,
        user: crdb_core::User,
        object_id: ObjectId,
        only_updated_since: Option<Updatedness>,
    ) -> crdb_core::Result<crdb_core::ObjectData> {
        self.db
            .get_all(connection, user, object_id, only_updated_since)
            .await
    }

    async fn server_query(
        &self,
        user: User,
        type_id: TypeId,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> crate::Result<Vec<ObjectId>> {
        self.db
            .server_query(user, type_id, only_updated_since, query)
            .await
    }

    async fn server_vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<SystemTime>,
        notify_recreation: impl FnMut(crdb_core::Update, HashSet<crdb_core::User>),
    ) -> crdb_core::Result<()> {
        // A server vacuum cannot change the latest snapshot, so there is nothing to update in the cache
        self.db
            .server_vacuum(
                no_new_changes_before,
                updatedness,
                kill_sessions_older_than,
                notify_recreation,
            )
            .await
    }

    async fn recreate_at<'a, T: Object, C: crdb_core::CanDoCallbacks>(
        &'a self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> crdb_core::Result<Option<(EventId, Arc<T>)>> {
        // A recreation at a specified timestamp cannot change the latest snapshot, so there is nothing to update in the cache
        self.db
            .recreate_at(object_id, event_id, updatedness, cb)
            .await
    }

    async fn create_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Updatedness,
    ) -> crdb_core::Result<Option<(Arc<T>, Vec<crdb_core::ReadPermsChanges>)>> {
        self.cache.write().unwrap().remove(&object_id);
        self.db
            .create_and_return_rdep_changes(object_id, created_at, object, updatedness)
            .await
    }

    async fn submit_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
    ) -> crdb_core::Result<Option<(Arc<T>, Vec<crdb_core::ReadPermsChanges>)>> {
        self.cache.write().unwrap().remove(&object_id);
        self.db
            .submit_and_return_rdep_changes(object_id, event_id, event, updatedness)
            .await
    }

    async fn update_pending_rdeps(&self) -> crdb_core::Result<()> {
        self.db.update_pending_rdeps().await
    }

    async fn login_session(
        &self,
        session: Session,
    ) -> crdb_core::Result<(SessionToken, SessionRef)> {
        self.db.login_session(session).await
    }

    async fn resume_session(&self, token: SessionToken) -> crdb_core::Result<Session> {
        self.db.resume_session(token).await
    }

    async fn mark_session_active(
        &self,
        token: SessionToken,
        at: SystemTime,
    ) -> crdb_core::Result<()> {
        self.db.mark_session_active(token, at).await
    }

    async fn rename_session<'a>(
        &'a self,
        token: SessionToken,
        new_name: &'a str,
    ) -> crdb_core::Result<()> {
        self.db.rename_session(token, new_name).await
    }

    async fn list_sessions(&self, user: User) -> crdb_core::Result<Vec<Session>> {
        self.db.list_sessions(user).await
    }

    async fn disconnect_session(&self, user: User, session: SessionRef) -> crdb_core::Result<()> {
        self.db.disconnect_session(user, session).await
    }
}
