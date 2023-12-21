use crate::{api::Query, Object};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObjectId(pub(crate) Uuid);
pub(crate) struct EventId(pub(crate) Uuid);
pub(crate) struct TypeId(pub(crate) Uuid);

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum MaybeParsed<T> {
    Json(Arc<serde_json::Value>),
    Parsed(Arc<T>),
}

#[derive(Clone)]
pub(crate) enum MaybeParsedAny {
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
    pub(crate) fn downcast<T: Any + Send + Sync>(self) -> anyhow::Result<MaybeParsed<T>> {
        Ok(match self {
            MaybeParsedAny::Json(v) => MaybeParsed::Json(v),
            MaybeParsedAny::Parsed(v) => MaybeParsed::Parsed(
                v.downcast()
                    .map_err(|_| anyhow::anyhow!("Failed downcasting to expected type"))?,
            ),
        })
    }
}

#[derive(Clone)]
pub(crate) struct FullObject {
    pub(crate) creation_time: Timestamp,
    pub(crate) creation: MaybeParsedAny,
    pub(crate) last_snapshot_time: Timestamp,
    pub(crate) last_snapshot: MaybeParsedAny,
    pub(crate) events: Arc<BTreeMap<Timestamp, Vec<MaybeParsedAny>>>,
}

#[derive(Clone, Copy)]
pub(crate) struct Timestamp(u64); // Nanoseconds since UNIX_EPOCH divided by 10

pub(crate) trait Db {
    fn set_new_object_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    );
    fn set_new_event_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
    );

    async fn create<T: Object>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        object: MaybeParsed<T>,
    ) -> anyhow::Result<()>;
    async fn submit<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: MaybeParsed<T::Event>,
    ) -> anyhow::Result<()>;

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject>;
    async fn query(
        &self,
        type_id: TypeId,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>>;

    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()>;
}
