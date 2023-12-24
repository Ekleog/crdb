use crate::{
    api::{BinPtr, Query},
    Object,
};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ObjectId(pub(crate) Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EventId(pub(crate) Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TypeId(pub(crate) Ulid);

#[derive(Clone)]
pub(crate) struct Changes {
    events: Vec<Arc<dyn Any + Send + Sync>>,
    snapshot_after: Option<Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone)]
pub(crate) struct FullObject {
    pub(crate) creation_time: Timestamp,
    pub(crate) creation: Arc<dyn Any + Send + Sync>,
    pub(crate) changes: Arc<BTreeMap<Timestamp, Changes>>,
}

impl FullObject {
    pub(crate) async fn apply(
        &mut self,
        time: Timestamp,
        event: Arc<dyn Any + Send + Sync>,
    ) -> anyhow::Result<()> {
        todo!()
    }
}

pub(crate) struct NewObject {
    time: Timestamp,
    type_id: TypeId,
    id: ObjectId,
    value: Arc<dyn Any + Send + Sync>,
}

pub(crate) struct NewEvent {
    time: Timestamp,
    type_id: TypeId,
    object_id: ObjectId,
    id: EventId,
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Clone, Copy)]
pub(crate) struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

pub(crate) trait Db {
    /// These functions are called whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn set_new_object_cb(&mut self, cb: Box<dyn Fn(NewObject)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn Fn(NewEvent)>);

    async fn create<T: Object>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        object: Arc<T>,
    ) -> anyhow::Result<()>;
    async fn submit<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()>;

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject>;
    async fn query(
        &self,
        type_id: TypeId,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>>;

    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()>;

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()>;
    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Arc<Vec<u8>>>;
}
