use super::Authenticator;
use crate::db_trait::Db;
use std::sync::Arc;

pub struct ApiDb<A: Authenticator> {
    _auth: A,
}

impl<A: Authenticator> ApiDb<A> {
    pub async fn connect(_base_url: Arc<String>, _auth: Arc<A>) -> anyhow::Result<ApiDb<A>> {
        todo!()
    }

    pub async fn disconnect(self) -> anyhow::Result<()> {
        todo!()
    }
}

#[allow(unused_variables)] // TODO: remove
impl<A: Authenticator> Db for ApiDb<A> {
    async fn new_objects(
        &self,
    ) -> impl Send + futures::Stream<Item = crate::crdb_internal::NewObject> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_events(
        &self,
    ) -> impl Send + futures::Stream<Item = crate::crdb_internal::NewEvent> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_snapshots(
        &self,
    ) -> impl Send + futures::Stream<Item = crate::crdb_internal::NewSnapshot> {
        // todo!()
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: crate::db_trait::ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create<T: crate::Object>(
        &self,
        object_id: crate::db_trait::ObjectId,
        object: std::sync::Arc<T>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn submit<T: crate::Object>(
        &self,
        object: crate::db_trait::ObjectId,
        event_id: crate::db_trait::EventId,
        event: std::sync::Arc<T::Event>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get<T: crate::Object>(
        &self,
        ptr: crate::db_trait::ObjectId,
    ) -> anyhow::Result<crate::db_trait::FullObject> {
        todo!()
    }

    async fn query<T: crate::Object>(
        &self,
        user: crate::User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<crate::Timestamp>,
        q: crate::Query,
    ) -> anyhow::Result<impl futures::Stream<Item = crate::db_trait::FullObject>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn snapshot<T: crate::Object>(
        &self,
        time: crate::Timestamp,
        object: crate::db_trait::ObjectId,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(
        &self,
        id: crate::BinPtr,
        value: std::sync::Arc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: crate::BinPtr) -> anyhow::Result<std::sync::Arc<Vec<u8>>> {
        todo!()
    }
}
