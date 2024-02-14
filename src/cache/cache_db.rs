use super::{BinariesCache, ObjectCache};
use crate::{
    db_trait::{Db, Lock},
    hash_binary, BinPtr, DynSized, EventId, Object, ObjectId, Updatedness,
};
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
    pub(crate) fn new(db: Arc<D>, watermark: usize) -> CacheDb<D> {
        let cache = Arc::new(RwLock::new(ObjectCache::new(watermark)));
        let this = CacheDb {
            db: db.clone(),
            cache,
            binaries: Arc::new(RwLock::new(BinariesCache::new())),
        };
        this
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
        lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        self.cache.write().unwrap().remove(&object_id);
        let res = self
            .db
            .create(object_id, created_at, object.clone(), updatedness, lock)
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
        force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .submit::<T>(object_id, event_id, event.clone(), updatedness, force_lock)
            .await?;
        if let Some(value) = res.clone() {
            self.cache.write().unwrap().set(object_id, value);
        }
        Ok(res)
    }

    async fn get_latest<T: Object>(
        &self,
        lock: Lock,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        if let Some(res) = self.cache.read().unwrap().get(&object_id) {
            if let Ok(res) = Arc::downcast(DynSized::arc_to_any(res)) {
                return Ok(res);
            }
            // Ignore wrong type errors, they'll be catched by self.db.get_latest below, where the real type will be known
        }
        let res = self.db.get_latest::<T>(lock, object_id).await?;
        self.cache.write().unwrap().set(object_id, res.clone() as _);
        Ok(res)
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .recreate(object_id, new_created_at, object, updatedness, force_lock)
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
}

impl<D: Db> Deref for CacheDb<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
