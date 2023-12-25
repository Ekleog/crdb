use crate::{
    api::{BinPtr, Query},
    traits::{Db, EventId, FullObject, NewEvent, NewObject, ObjectId, Timestamp, TypeId},
    Object, User,
};
use anyhow::Context;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;

pub(crate) struct Cache<D: Db> {
    db: D,
    // TODO: figure out how to purge from cache (LRU-style), using DeepSizeOf
    cache: RwLock<HashMap<ObjectId, FullObject>>,
    binaries: RwLock<HashMap<BinPtr, Arc<Vec<u8>>>>,
}

impl<D: Db> Cache<D> {
    pub(crate) fn new(db: D) -> Cache<D> {
        Self {
            db,
            cache: RwLock::new(HashMap::new()),
            binaries: RwLock::new(HashMap::new()),
        }
    }
}

impl<D: Db> Db for Cache<D> {
    fn set_new_object_cb(&mut self, cb: Box<dyn Fn(NewObject) -> bool>) {
        self.db.set_new_object_cb(cb);
        todo!()
    }

    fn set_new_event_cb(&mut self, cb: Box<dyn Fn(NewEvent) -> bool>) {
        self.db.set_new_event_cb(cb);
        todo!()
    }

    async fn create<T: Object>(&self, object_id: ObjectId, object: Arc<T>) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        let cache_entry = cache.entry(object_id);
        match cache_entry {
            hash_map::Entry::Occupied(o) => {
                anyhow::ensure!(
                    o.get()
                        .creation
                        .clone()
                        .downcast::<T>()
                        .map(|v| v == object)
                        .unwrap_or(false),
                    "Object {object_id:?} was already created with a different initial value"
                );
            }
            hash_map::Entry::Vacant(v) => {
                self.db.create(object_id, object.clone()).await?;
                v.insert(FullObject {
                    id: object_id,
                    created_at: EventId(object_id.0),
                    creation: object,
                    changes: Arc::new(BTreeMap::new()),
                });
            }
        }
        Ok(())
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        self.db
            .submit::<T>(object_id, event_id, event.clone())
            .await?;
        match cache.entry(object_id) {
            hash_map::Entry::Occupied(mut object) => {
                object
                    .get_mut()
                    .apply::<T>(event_id, event)
                    .await
                    .with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
            }
            hash_map::Entry::Vacant(v) => {
                let o = self
                    .db
                    .get(object_id)
                    .await
                    .with_context(|| format!("getting {object_id:?} from database"))?;
                v.insert(o);
            }
        };
        Ok(())
    }

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject> {
        {
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&ptr) {
                return Ok(res.clone());
            }
        }
        // TODO: subscribe to new events on ptr
        let res = self.db.get(ptr).await?;
        {
            let mut cache = self.cache.write().await;
            cache.insert(ptr, res.clone());
        }
        Ok(res)
    }

    async fn query(
        &self,
        type_id: TypeId,
        user: User,
        include_heavy: bool,
        q: Query,
    ) -> anyhow::Result<impl futures::Stream<Item = FullObject>> {
        todo!();
        Ok(futures::stream::empty())
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        if let Some(o) = cache.get_mut(&object) {
            o.snapshot::<T>(time)?;
        }
        self.db.snapshot::<T>(time, object).await
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        let mut binaries = self.binaries.write().await;
        binaries.insert(id, value.clone());
        self.db.create_binary(id, value).await
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Arc<Vec<u8>>> {
        {
            let binaries = self.binaries.read().await;
            if let Some(res) = binaries.get(&ptr) {
                return Ok(res.clone());
            }
        }
        let res = self.db.get_binary(ptr).await?;
        {
            let mut binaries = self.binaries.write().await;
            binaries.insert(ptr, res.clone());
        }
        Ok(res)
    }
}
