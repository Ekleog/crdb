use crate::{
    api::{BinPtr, Query},
    traits::{Db, EventId, FullObject, MaybeParsed, MaybeParsedAny, ObjectId, Timestamp, TypeId},
    Object,
};
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;

pub(crate) struct Cache<D: Db> {
    db: D,
    // TODO: figure out how to purge from cache (LRU-style)
    cache: RwLock<HashMap<ObjectId, FullObject>>,
    new_object_cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    new_event_cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
}

#[allow(unused_variables)] // TODO: remove once impl'd
impl<D: Db> Db for Cache<D> {
    fn set_new_object_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    ) {
        self.new_object_cb = cb;
    }

    fn set_new_event_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
    ) {
        self.new_event_cb = cb;
    }

    async fn create<T: Object>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        object: MaybeParsed<T>,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        let cache_entry = cache.entry(object_id);
        match cache_entry {
            hash_map::Entry::Occupied(o) => {
                anyhow::ensure!(
                    o.get().creation.clone().downcast::<T>()? == object,
                    "Object {object_id:?} was already created with a different initial value"
                );
            }
            hash_map::Entry::Vacant(v) => {
                let object_any = MaybeParsedAny::from(object.clone());
                self.db.create(time, object_id, object).await?;
                v.insert(FullObject {
                    creation_time: time,
                    creation: object_any.clone(),
                    changes: Arc::new(BTreeMap::new()),
                });
            }
        }
        Ok(())
    }

    async fn submit<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: MaybeParsed<T::Event>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject> {
        {
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&ptr) {
                return Ok(res.clone());
            }
        }
        // TODO: subscribe to new events on ptr
        // TODO: clients must also start by populating the cache with all non-heavy objects?
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
        q: Query,
    ) -> anyhow::Result<impl futures::Stream<Item = FullObject>> {
        todo!();
        Ok(futures::stream::empty())
    }

    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: ulid::Ulid, value: &[u8]) -> anyhow::Result<BinPtr> {
        todo!()
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Vec<u8>> {
        todo!()
    }
}
