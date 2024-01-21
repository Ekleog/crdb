use super::{ApiDb, Authenticator, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ClientDb<A: Authenticator> {
    api: Arc<ApiDb<A>>,
    db: Arc<CacheDb<LocalDb>>,
    vacuum_guard: RwLock<()>,
}

impl<A: Authenticator> ClientDb<A> {
    pub async fn connect<C: ApiConfig>(
        base_url: Arc<String>,
        auth: Arc<A>,
        local_db: &str,
        cache_watermark: usize,
    ) -> anyhow::Result<ClientDb<A>> {
        // TODO(client): Make user configure a vacuum schedule, and lock vacuuming on a write lock on the
        // vacuum_guard here. Maybe have the vacuum be skipped unless there's enough storage used, as per
        // https://developer.mozilla.org/en-US/docs/Web/API/StorageManager/estimate
        C::check_ulids();
        let api = Arc::new(ApiDb::connect(base_url, auth).await?);
        let db = CacheDb::new::<C>(Arc::new(LocalDb::connect(local_db).await?), cache_watermark);
        db.watch_from::<C, _>(&api);
        Ok(ClientDb {
            api,
            db,
            vacuum_guard: RwLock::new(()),
        })
    }

    pub fn user(&self) -> User {
        self.api.user()
    }

    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.api.disconnect().await
    }

    pub async fn pause_vacuum(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.vacuum_guard.read().await
    }

    pub async fn clear_cache(&self) {
        self.db.clear_cache().await
    }

    pub async fn clear_binaries_cache(&self) {
        self.db.clear_binaries_cache().await
    }

    pub async fn clear_objects_cache(&self) {
        self.db.clear_objects_cache().await
    }

    pub async fn reduce_size_to(&mut self, size: usize) {
        self.db.reduce_size_to(size).await
    }

    pub async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        self.api.new_objects().await
    }

    pub async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        self.api.new_events().await
    }

    pub async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        self.api.new_recreations().await
    }

    pub async fn unlock(&self, ptr: ObjectId) -> crate::Result<()> {
        self.db.unlock(ptr).await
    }

    pub async fn unsubscribe(&self, ptr: ObjectId) -> crate::Result<()> {
        self.db.remove(ptr).await?;
        self.api.unsubscribe(ptr).await
    }

    pub async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<()> {
        self.api.create(id, created_at, object.clone()).await?;
        self.db
            .create(id, created_at, object, true, &*self.db)
            .await
    }

    pub async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<()> {
        self.api
            .submit::<T>(object, event_id, event.clone())
            .await?;
        self.db
            .submit::<T, _>(object, event_id, event, &*self.db)
            .await
    }

    pub async fn get<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<FullObject> {
        match self.db.get::<T>(lock, object_id).await {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let res = self.api.get::<T>(object_id).await?;
        self.db
            .create_all::<T, _>(res.clone(), lock, &*self.db)
            .await?;
        Ok(res)
    }

    pub async fn query_local<'a, T: Object>(
        &'a self,
        user: User,
        q: &'a Query,
    ) -> crate::Result<impl 'a + CrdbStream<Item = crate::Result<FullObject>>> {
        self.db.query::<T>(user, None, q).await
    }

    pub async fn query_remote<T: Object>(
        &self,
        lock: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<FullObject>>> {
        Ok(self
            .api
            .query::<T>(self.user(), ignore_not_modified_on_server_since, q)
            .await?
            .then({
                let db = self.db.clone();
                move |o| {
                    let db = db.clone();
                    async move {
                        let o = o?;
                        db.create_all::<T, _>(o.clone(), lock, &*db).await?;
                        Ok(o)
                    }
                }
            }))
    }

    pub async fn recreate<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
    ) -> crate::Result<()> {
        self.db.recreate::<T, _>(time, object, &*self.db).await?;
        self.api.recreate::<T>(time, object).await
    }

    pub async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        self.db.create_binary(binary_id, data.clone()).await?;
        self.api.create_binary(binary_id, data).await
    }

    pub async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.db.get_binary(binary_id).await? {
            return Ok(Some(res));
        }
        let Some(res) = self.api.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.db.create_binary(binary_id, res.clone()).await?;
        Ok(Some(res))
    }
}
