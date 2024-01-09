use super::{BinariesCache, CacheConfig, ObjectCache};
use crate::{
    db_trait::Db, error::ResultExt, full_object::FullObject, hash_binary, BinPtr, CanDoCallbacks,
    CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct CacheDb<D: Db> {
    db: Arc<D>,
    cache: Arc<RwLock<ObjectCache>>,
    binaries: Arc<RwLock<BinariesCache>>,
}

impl<D: Db> CacheDb<D> {
    #[cfg(feature = "client")]
    pub fn watch_from<C: CacheConfig, A: crate::client::Authenticator>(
        self: &Arc<Self>,
        api: &Arc<crate::client::ApiDb<A>>,
    ) {
        use futures::pin_mut;

        // Watch new objects
        crate::spawn({
            let api = api.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            let this = self.clone();
            async move {
                let new_objects = api.new_objects().await;
                pin_mut!(new_objects);
                while let Some(o) = new_objects.next().await {
                    let mut cache = cache.write().await;
                    let object = o.id;
                    if let Err(error) = C::create_in_db(&*internal_db, o.clone(), &*this).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed creating received object in internal db"
                        );
                    }
                    if let Err(error) = C::create(&mut *cache, o).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed creating received object in cache"
                        );
                    }
                }
            }
        });

        // Watch new events
        crate::spawn({
            let api = api.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            let this = self.clone();
            async move {
                let new_events = api.new_events().await;
                pin_mut!(new_events);
                while let Some(e) = new_events.next().await {
                    let mut cache = cache.write().await;
                    let object = e.object_id;
                    let event = e.id;
                    if let Err(error) = C::submit_in_db(&*internal_db, e.clone(), &*this).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            ?event,
                            "failed submitting received object to internal db"
                        );
                    }
                    // DO NOT re-fetch object when receiving an event not in cache for it.
                    // Without this, users would risk unsubscribing from an object, then receiving
                    // an event on this object (as a race condition), and then staying subscribed.
                    if let Err(error) = C::submit(&mut *cache, e).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            ?event,
                            "failed submitting received event to cache"
                        );
                    }
                }
            }
        });

        // Watch new re-creations
        crate::spawn({
            let api = api.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            let this = self.clone();
            async move {
                let new_recreations = api.new_recreations().await;
                pin_mut!(new_recreations);
                while let Some(s) = new_recreations.next().await {
                    let mut cache = cache.write().await;
                    let object = s.object_id;
                    if let Err(error) = C::recreate_in_db(&*internal_db, s.clone(), &*this).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed recreating as per received event in internal db"
                        );
                    }
                    if let Err(error) = C::recreate(&mut *cache, s).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed recreating as per received event in cache"
                        )
                    }
                }
            }
        });
    }

    /// Note that `watermark` will still keep in the cache all objects still in use by the program.
    /// So, setting `watermark` to a value too low (eg. less than the size of objects actually in use by the
    /// program) would make cache operation slow.
    /// For this reason, if the cache used size reaches the watermark, then the watermark will be
    /// automatically increased.
    pub(crate) fn new<C: CacheConfig>(db: Arc<D>, watermark: usize) -> Arc<CacheDb<D>> {
        let cache = Arc::new(RwLock::new(ObjectCache::new(watermark)));
        let this = Arc::new(CacheDb {
            db: db.clone(),
            cache,
            binaries: Arc::new(RwLock::new(BinariesCache::new())),
        });
        this
    }

    pub async fn clear_cache(&self) {
        self.clear_binaries_cache().await;
        self.clear_objects_cache().await;
    }

    pub async fn clear_binaries_cache(&self) {
        self.binaries.write().await.clear();
    }

    pub async fn clear_objects_cache(&self) {
        self.cache.write().await.clear();
    }

    pub async fn reduce_size_to(&self, size: usize) {
        self.cache.write().await.reduce_size_to(size);
        self.binaries.write().await.clear();
        // TODO: auto-clear binaries without waiting for a reduce_size_to call
        // Probably both cache & binaries should be dealt with together by moving the
        // watermark handling to the CacheDb level
    }

    #[cfg(feature = "client")]
    pub async fn create_all<T: Object, C: CanDoCallbacks>(
        &self,
        o: FullObject,
        cb: &C,
    ) -> crate::Result<()> {
        let (creation, changes) = o.extract_all_clone();
        self.create::<T, _>(
            creation.id,
            creation.created_at,
            creation
                .creation
                .arc_to_any()
                .downcast::<T>()
                .expect("Type provided to `CacheDb::create_all` is wrong"),
            cb,
        )
        .await?;
        for (event_id, c) in changes.into_iter() {
            self.submit::<T, _>(
                creation.id,
                event_id,
                c.event
                    .arc_to_any()
                    .downcast::<T::Event>()
                    .expect("Type provided to `CacheDb::create_all` is wrong"),
                cb,
            )
            .await?;
        }
        Ok(())
    }
}

impl<D: Db> Db for CacheDb<D> {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> crate::Result<()> {
        // First change db, then the cache, because db can rely on the `get`s from the
        // cache to compute `users_who_can_read`, and this in turn means that the cache
        // should never (lock reads or) return not up-to-date information, nor return
        // information that has not yet been validated by the database.
        self.db.create(id, created_at, object.clone(), cb).await?;
        self.cache.write().await.create(id, created_at, object)?;
        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<()> {
        // First change db, then the cache, because db can rely on the `get`s from the
        // cache to compute `users_who_can_read`, and this in turn means that the cache
        // should never (lock reads or) return not up-to-date information, nor return
        // information that has not yet been validated by the database.
        self.db
            .submit::<T, _>(object_id, event_id, event.clone(), cb)
            .await?;
        self.cache
            .write()
            .await
            .submit::<T>(object_id, event_id, event)?;
        Ok(())
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        {
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&object_id) {
                return Ok(res.clone());
            }
        }
        let res = self.db.get::<T>(object_id).await?;
        debug_assert!(
            res.id() == object_id,
            "Got result with id {:?} instead of expected id {object_id:?}",
            res.id()
        );
        {
            let mut cache = self.cache.write().await;
            cache
                .insert::<T>(res.clone())
                .wrap_with_context(|| format!("inserting object {object_id:?} in the cache"))?;
        }
        Ok(res)
    }

    async fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // We cannot use the object cache here, because it is not guaranteed to contain
        // all the queried objects, due to being an LRU cache. So, immediately delegate
        // to the underlying database, which should forward to either PostgreSQL for the
        // server, or IndexedDB/SQLite for the client.
        Ok(self
            .db
            .query::<T>(user, ignore_not_modified_on_server_since, q)
            .await?
            .then(|o| async {
                let o = o?;
                let mut cache = self.cache.write().await;
                if let Err(error) = cache.insert::<T>(o.clone()) {
                    let id = o.id();
                    tracing::error!(?id, ?error, "failed inserting queried object in cache");
                    cache.remove(&id);
                }
                Ok(o)
            }))
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        self.db.recreate::<T, C>(time, object, cb).await?;
        self.cache.write().await.recreate::<T>(object, time)
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        debug_assert!(
            binary_id == hash_binary(&*data),
            "Provided id {binary_id:?} does not match value hash {:?}",
            hash_binary(&*data),
        );
        self.binaries.write().await.insert(binary_id, data.clone());
        self.db.create_binary(binary_id, data).await
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.binaries.read().await.get(&binary_id) {
            return Ok(Some(res.clone()));
        }
        let Some(res) = self.db.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.binaries.write().await.insert(binary_id, res.clone());
        Ok(Some(res))
    }
}
