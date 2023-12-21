use crate::{
    traits::{CachedObject, Db, EventId, MaybeParsed, MaybeParsedAny, ObjectId, Timestamp, TypeId},
    Object,
};
use std::collections::{hash_map, BTreeMap, HashMap};
use tokio::sync::RwLock;

pub(crate) struct Cache<D: Db> {
    db: D,
    cache: RwLock<HashMap<ObjectId, CachedObject>>,
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
        {
            // Restrict the lock lifetime
            let mut cache = self.cache.write().await;
            let cache_entry = cache.entry(object_id);
            let object_any = MaybeParsedAny::from(object.clone());
            match cache_entry {
                hash_map::Entry::Occupied(o) => {
                    anyhow::ensure!(
                        o.get().creation.clone().downcast::<T>()? == object,
                        "Object {object_id:?} was already created with a different initial value"
                    );
                }
                hash_map::Entry::Vacant(v) => {
                    v.insert(CachedObject {
                        creation_time: time,
                        creation: object_any.clone(),
                        last_snapshot_time: time,
                        last_snapshot: object_any,
                        events: BTreeMap::new(),
                    });
                }
            }
        }
        self.db.create(time, object_id, object).await
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<MaybeParsed<T>> {
        {
            // Restrict the lock lifetime
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&ptr) {
                return Ok(res.last_snapshot.clone().downcast()?);
            }
        }
        let res = self.db.get(ptr).await?;
        {
            // Restrict the lock lifetime
            let mut cache = self.cache.write().await;
            cache.insert(ptr, todo!());
        }
        Ok(res)
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

    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }
}
