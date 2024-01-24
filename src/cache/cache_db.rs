use super::{BinariesCache, ObjectCache};
use crate::{
    db_trait::Db, error::ResultExt, full_object::FullObject, hash_binary, BinPtr, CanDoCallbacks,
    EventId, Object, ObjectId, Query, Timestamp, User,
};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct CacheDb<D: Db> {
    db: Arc<D>,
    cache: Arc<RwLock<ObjectCache>>,
    binaries: Arc<RwLock<BinariesCache>>,
}

impl<D: Db> CacheDb<D> {
    /// Note that `watermark` will still keep in the cache all objects still in use by the program.
    /// So, setting `watermark` to a value too low (eg. less than the size of objects actually in use by the
    /// program) would make cache operation slow.
    /// For this reason, if the cache used size reaches the watermark, then the watermark will be
    /// automatically increased.
    pub(crate) fn new(db: Arc<D>, watermark: usize) -> Arc<CacheDb<D>> {
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
        // TODO(low): auto-clear binaries without waiting for a reduce_size_to call
        // Probably both cache & binaries should be dealt with together by moving the
        // watermark handling to the CacheDb level
    }

    #[cfg(feature = "client")]
    pub async fn create_all<T: Object, C: CanDoCallbacks>(
        &self,
        o: FullObject,
        lock: bool,
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
            lock,
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
        lock: bool,
        cb: &C,
    ) -> crate::Result<()> {
        // First change db, then the cache, because db can rely on the `get`s from the
        // cache to compute `users_who_can_read`, and this in turn means that the cache
        // should never (lock reads or) return not up-to-date information, nor return
        // information that has not yet been validated by the database.
        self.db
            .create(id, created_at, object.clone(), lock, cb)
            .await?;
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

    async fn get<T: Object>(&self, lock: bool, object_id: ObjectId) -> crate::Result<FullObject> {
        {
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&object_id) {
                return Ok(res.clone());
            }
        }
        let res = self.db.get::<T>(lock, object_id).await?;
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
        only_updated_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<Vec<ObjectId>> {
        // We cannot use the object cache here, because it is not guaranteed to contain
        // all the queried objects, due to being an LRU cache. So, immediately delegate
        // to the underlying database, which should forward to either PostgreSQL for the
        // server, or IndexedDB/SQLite for the client.
        self.db.query::<T>(user, only_updated_since, q).await
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

    async fn unlock(&self, object_id: ObjectId) -> crate::Result<()> {
        self.db.unlock(object_id).await
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        self.cache.write().await.remove(&object_id);
        self.db.remove(object_id).await
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        debug_assert!(
            binary_id == hash_binary(&*data),
            "Provided id {binary_id:?} does not match value hash {:?}",
            hash_binary(&*data),
        );
        self.binaries
            .write()
            .await
            .insert(binary_id, Arc::downgrade(&data));
        self.db.create_binary(binary_id, data).await
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
        if let Some(res) = self.binaries.read().await.get(&binary_id) {
            return Ok(Some(res.clone()));
        }
        let Some(res) = self.db.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.binaries
            .write()
            .await
            .insert(binary_id, Arc::downgrade(&res));
        Ok(Some(res))
    }
}
