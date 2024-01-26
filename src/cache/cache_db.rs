use super::{BinariesCache, ObjectCache};
use crate::{
    db_trait::Db, hash_binary, BinPtr, CanDoCallbacks, DynSized, EventId, Object, ObjectId,
};
use anyhow::anyhow;
use std::{
    ops::Deref,
    sync::{Arc, RwLock},
};

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

    // TODO(low): auto-clear binaries without waiting for a reduce_size_to call
    // Probably both cache & binaries should be dealt with together by moving the
    // watermark handling to the CacheDb level. OTOH binaries are already Weak so
    // it's not a big deal
}

impl<D: Db> Db for CacheDb<D> {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        self.cache.write().unwrap().remove(&object_id);
        let res = self
            .db
            .create(object_id, created_at, object.clone(), lock, cb)
            .await?;
        if let Some(value) = res.clone() {
            self.cache.write().unwrap().set(object_id, value);
        }
        Ok(res)
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .submit::<T, _>(object_id, event_id, event.clone(), cb)
            .await?;
        if let Some(value) = res.clone() {
            self.cache.write().unwrap().set(object_id, value);
        }
        Ok(res)
    }

    async fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        if let Some(res) = self.cache.read().unwrap().get(&object_id) {
            let res = Arc::downcast(DynSized::arc_to_any(res)).map_err(|_| {
                crate::Error::Other(anyhow!("requested object with the wrong type"))
            })?;
            return Ok(res);
        }
        let res = self.db.get_latest::<T>(lock, object_id).await?;
        self.cache.write().unwrap().set(object_id, res.clone() as _);
        Ok(res)
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        force_lock: bool,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .recreate::<T, C>(object_id, new_created_at, object, force_lock, cb)
            .await?;
        if let Some(res) = res.clone() {
            self.cache.write().unwrap().set(object_id, res as _);
        }
        Ok(res)
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        self.cache.write().unwrap().remove(&object_id);
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
            .unwrap()
            .insert(binary_id, Arc::downgrade(&data));
        self.db.create_binary(binary_id, data).await
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
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
}

impl<D: Db> Deref for CacheDb<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
