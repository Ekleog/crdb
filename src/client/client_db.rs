use super::{ApiDb, Authenticator};
use crate::{
    db_trait::{Db, EventId, FullObject, NewEvent, NewObject, NewSnapshot, ObjectId},
    BinPtr, Object, Query, Timestamp, User,
};
use futures::Stream;
use std::sync::Arc;

#[doc(hidden)]
pub struct ClientDb<A: Authenticator> {
    api: ApiDb<A>,
}

impl<A: Authenticator> ClientDb<A> {
    pub async fn connect(base_url: Arc<String>, auth: Arc<A>) -> anyhow::Result<ClientDb<A>> {
        Ok(ClientDb {
            api: ApiDb::connect(base_url, auth).await?,
        })
    }

    pub async fn disconnect(self) -> anyhow::Result<()> {
        self.api.disconnect().await
    }
}

#[allow(unused_variables)] // TODO: remove
impl<A: Authenticator> Db for ClientDb<A> {
    async fn new_objects(&self) -> impl Stream<Item = NewObject> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = NewEvent> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_snapshots(&self) -> impl Send + Stream<Item = NewSnapshot> {
        // todo!()
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create<T: Object>(&self, object_id: ObjectId, object: Arc<T>) -> anyhow::Result<()> {
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

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Arc<Vec<u8>>> {
        todo!()
    }
}
