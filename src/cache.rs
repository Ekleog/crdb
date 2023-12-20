use crate::{Db, EventId, MaybeParsed, Object, ObjectId, Timestamp, TypeId};
use std::{
    any::Any,
    collections::{hash_map, BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::RwLock;

#[derive(Clone)]
enum MaybeParsedAny {
    Json(Arc<serde_json::Value>),
    Parsed(Arc<dyn Any + Send + Sync>),
}

impl<T: Any + Send + Sync> From<MaybeParsed<T>> for MaybeParsedAny {
    fn from(value: MaybeParsed<T>) -> Self {
        match value {
            MaybeParsed::Json(v) => MaybeParsedAny::Json(v),
            MaybeParsed::Parsed(v) => MaybeParsedAny::Parsed(v),
        }
    }
}

impl MaybeParsedAny {
    fn downcast<T: Any + Send + Sync>(self) -> anyhow::Result<MaybeParsed<T>> {
        Ok(match self {
            MaybeParsedAny::Json(v) => MaybeParsed::Json(v),
            MaybeParsedAny::Parsed(v) => MaybeParsed::Parsed(
                v.downcast()
                    .map_err(|_| anyhow::anyhow!("Failed downcasting to expected type"))?,
            ),
        })
    }
}

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
        todo!()
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

struct CachedObject {
    creation_time: Timestamp,
    creation: MaybeParsedAny,
    last_snapshot_time: Timestamp,
    last_snapshot: MaybeParsedAny,
    events: BTreeMap<Timestamp, Vec<MaybeParsedAny>>,
}
