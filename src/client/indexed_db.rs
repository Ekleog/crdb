use crate::{
    db_trait::{Db, EventId, FullObject, NewEvent, NewObject, NewRecreation, ObjectId},
    BinPtr, Object, Query, Timestamp, User,
};
use futures::Stream;
use std::sync::Arc;

pub struct IndexedDb {
    // TODO
}

impl IndexedDb {
    pub fn new() -> IndexedDb {
        todo!()
    }
}

#[allow(unused_variables)] // TODO: remove
impl Db for IndexedDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = NewObject> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = NewEvent> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl Send + Stream<Item = NewRecreation> {
        // todo!()
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
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

    async fn recreate<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
