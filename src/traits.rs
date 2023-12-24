use crate::{
    api::{BinPtr, Query},
    Object,
};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObjectId(pub(crate) Ulid);
pub(crate) struct EventId(pub(crate) Ulid);
pub(crate) struct TypeId(pub(crate) Ulid);

#[derive(Eq, PartialEq)]
pub(crate) enum MaybeParsed<T: ?Sized> {
    Json(Arc<serde_json::Value>),
    Parsed(Arc<T>),
}

pub(crate) type MaybeParsedAny = MaybeParsed<dyn Any + Send + Sync>;

impl<T: ?Sized> Clone for MaybeParsed<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Json(v) => Self::Json(v.clone()),
            Self::Parsed(v) => Self::Parsed(v.clone()),
        }
    }
}

impl<T: Any + Send + Sync> From<MaybeParsed<T>> for MaybeParsedAny {
    fn from(value: MaybeParsed<T>) -> Self {
        match value {
            MaybeParsed::Json(v) => MaybeParsed::Json(v),
            MaybeParsed::Parsed(v) => MaybeParsed::Parsed(v),
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
pub(crate) struct Changes {
    events: Vec<MaybeParsedAny>,
    snapshot_after: Option<MaybeParsedAny>,
}

#[derive(Clone)]
pub(crate) struct FullObject {
    pub(crate) creation_time: Timestamp,
    pub(crate) creation: MaybeParsedAny,
    pub(crate) changes: Arc<BTreeMap<Timestamp, Changes>>,
}

#[derive(Clone, Copy)]
pub(crate) struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

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

    async fn create_binary(&self, id: Ulid, value: &[u8]) -> anyhow::Result<BinPtr>;
    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Vec<u8>>;
}