use std::sync::Arc;

use crate::{
    db_trait::{Db, EventId, FullObject, NewEvent, NewObject, NewSnapshot, ObjectId, Timestamp},
    Object, User,
};
use anyhow::Context;
use futures::Stream;

pub(crate) struct SqlDb {
    _db: sqlx::PgPool,
}

impl SqlDb {
    pub async fn connect(db_url: &str) -> anyhow::Result<SqlDb> {
        Ok(SqlDb {
            _db: sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // TODO: make configurable (builder pattern?)
                .connect(&db_url)
                .await
                .with_context(|| format!("opening database {db_url:?}"))?,
        })
    }
}

// TODO: add a mechanism to auto-recreate all objects after some time elapsed
// TODO: add a mechanism to GC binaries that are no longer required after object re-creation

#[allow(unused_variables)] // TODO: remove
impl Db for SqlDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = NewObject> {
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
        // TODO: create a new snapshot with is_creation = true
    }

    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
        todo!()
        // TODO: add the event, create a new snapshot with is_creation = false for just after `event`,
        // and the last snapshot should be is_last = true. Also remove is_last from no-longer-last
        // snapshot
    }
    // TODO: make sure there is a postgresql ASSERT that validates that any newly-added BinPtr is
    // properly present in the same transaction as we're adding the event, reject if not.

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: crate::Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: crate::BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: crate::BinPtr) -> anyhow::Result<Arc<Vec<u8>>> {
        todo!()
    }
}
