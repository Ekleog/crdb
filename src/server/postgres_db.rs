use std::sync::Arc;

use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewSnapshot, EventId, ObjectId, Timestamp},
    full_object::FullObject,
    Object, User,
};
use anyhow::Context;
use futures::Stream;

use super::{Session, SessionRef, SessionToken};

#[cfg(test)]
mod tests;

pub(crate) struct PostgresDb {
    db: sqlx::PgPool,
}

#[allow(unused_variables, dead_code)] // TODO: remove
impl PostgresDb {
    pub async fn connect(db: sqlx::postgres::PgPool) -> anyhow::Result<PostgresDb> {
        sqlx::migrate!("src/server/migrations")
            .run(&db)
            .await
            .context("running migrations on postgresql database")?;
        Ok(PostgresDb { db })
    }

    pub async fn login_session(&self, session: Session) -> anyhow::Result<SessionToken> {
        todo!()
    }

    pub async fn retrieve_session(&self, token: SessionToken) -> anyhow::Result<User> {
        todo!()
    }

    pub async fn rename_session(&self, token: SessionToken, new_name: &str) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn list_sessions(&self, user: User) -> anyhow::Result<Vec<(SessionRef, Session)>> {
        todo!()
    }

    pub async fn disconnect_session_token(&self, token: SessionToken) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn disconnect_session_ref(&self, session: SessionRef) -> anyhow::Result<()> {
        todo!()
    }
}

// TODO: add a mechanism to auto-recreate all objects after some time elapsed
// TODO: add a mechanism to GC binaries that are no longer required after object re-creation

#[allow(unused_variables)] // TODO: remove
impl Db for PostgresDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = DynNewObject> {
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = DynNewEvent> {
        futures::stream::empty()
    }

    async fn new_snapshots(&self) -> impl Send + Stream<Item = DynNewSnapshot> {
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        unimplemented!("unsubscribing from a postgresql db does not make sense")
    }

    async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<()> {
        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let created_at = created_at.to_uuid();
        let object_id = id.to_uuid();
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        let users_who_can_read = object
            .users_who_can_read(&self)
            .await
            .context("listing users who can read")?;
        let is_heavy = object.is_heavy();
        let required_binaries = object.required_binaries();
        let affected =
            sqlx::query("INSERT INTO snapshots VALUES ($1, $2, TRUE, TRUE, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING")
                .bind(created_at)
                .bind(object_id)
                .bind(snapshot_version)
                .bind(object_json)
                .bind(users_who_can_read)
                .bind(is_heavy)
                .bind(required_binaries)
                .execute(&self.db)
                .await
                .with_context(|| format!("inserting snapshot {created_at:?}"))?
                .rows_affected();
        anyhow::ensure!(affected <= 1, "Object {id:?} already existed in database");
        if affected == 0 {
            // Check for equality with pre-existing
            let affected = sqlx::query(
                "
                    SELECT 1 FROM snapshots
                    WHERE snapshot_id = $1
                    AND object_id = $2
                    AND is_creation = TRUE
                    AND snapshot_version = $3
                    AND snapshot = $4
                ",
            )
            .bind(created_at)
            .bind(object_id)
            .bind(snapshot_version)
            .bind(object_json)
            .execute(&self.db)
            .await
            .with_context(|| {
                format!("checking pre-existing snapshot for {created_at:?} is the same")
            })?
            .rows_affected();
            anyhow::ensure!(
                affected == 1,
                "Snapshot {created_at:?} already existed with a different value set"
            );
        }
        Ok(())
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

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: crate::Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create_binary(&self, id: crate::BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_binary(&self, ptr: crate::BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
