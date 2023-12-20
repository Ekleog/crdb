use crate::{Db, EventId, MaybeParsed, ObjectId, Timestamp, TypeId};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

enum MaybeParsedAny {
    Json(serde_json::Value),
    Parsed(Arc<dyn Any + Send + Sync>),
}

pub(crate) struct Cache<D: Db> {
    db: D,
    cache: HashMap<ObjectId, CachedObject>,
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

    async fn create<T: crate::Object>(
        &self,
        time: crate::Timestamp,
        object_id: ObjectId,
        object: MaybeParsed<T>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get<T: crate::Object>(&self, ptr: ObjectId) -> anyhow::Result<MaybeParsed<T>> {
        todo!()
    }

    async fn submit<T: crate::Object>(
        &self,
        time: crate::Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: MaybeParsed<T::Event>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn snapshot(&self, time: crate::Timestamp, object: ObjectId) -> anyhow::Result<()> {
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
