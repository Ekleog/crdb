use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation, EventId, ObjectId, DbOpError},
    full_object::FullObject,
    BinPtr, Object, Query, Timestamp, User, CanDoCallbacks,
};
use futures::Stream;
use std::sync::Arc;

pub struct IndexedDb {
    // TODO
}

impl IndexedDb {
    pub async fn connect(_url: &str) -> anyhow::Result<IndexedDb> {
        todo!()
    }
}

#[allow(unused_variables)] // TODO: remove
impl Db for IndexedDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = DynNewObject> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = DynNewEvent> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl Send + Stream<Item = DynNewRecreation> {
        // todo!()
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> anyhow::Result<(), DbOpError> {
        todo!()
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> anyhow::Result<(), DbOpError> {
        todo!()
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
