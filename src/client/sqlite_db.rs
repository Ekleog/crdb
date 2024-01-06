use crate::{
    db_trait::{Db, DbOpError, DynNewEvent, DynNewObject, DynNewRecreation, EventId, ObjectId, TypeId},
    full_object::FullObject,
    BinPtr, CanDoCallbacks, Object, Query, Timestamp, User,
};
use anyhow::{Context, anyhow};
use futures::Stream;
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub struct SqliteDb {
    db: sqlx::SqlitePool,
}

impl SqliteDb {
    pub async fn connect_impl(db: sqlx::SqlitePool) -> anyhow::Result<SqliteDb> {
        sqlx::migrate!("src/client/migrations")
            .run(&db)
            .await
            .context("running migrations on sqlite database")?;
        Ok(SqliteDb { db })
    }

    pub async fn connect(url: &str) -> anyhow::Result<SqliteDb> {
        Self::connect_impl(sqlx::SqlitePool::connect(url).await?).await
    }
}

#[allow(unused_variables)] // TODO: remove
impl Db for SqliteDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = DynNewObject> {
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = DynNewEvent> {
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl Send + Stream<Item = DynNewRecreation> {
        futures::stream::empty()
    }

    async fn unsubscribe(&self, _ptr: ObjectId) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> Result<(), DbOpError> {
        reord::point().await;
        let mut t = self
            .db
            .begin()
            .await
            .context("acquiring sqlite transaction")
            .map_err(DbOpError::Other)?;

        // TODO add reord lock over whole database

        reord::point().await;
        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = TypeId(*T::type_ulid());
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        let is_heavy = object.is_heavy();
        reord::point().await;
        let affected = sqlx::query(
            "INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6)
                         ON CONFLICT DO NOTHING",
        )
        .bind(created_at)
        .bind(type_id)
        .bind(object_id)
        .bind(snapshot_version)
        .bind(object_json)
        .bind(is_heavy)
        .execute(&mut *t)
        .await
        .with_context(|| format!("inserting snapshot {created_at:?}"))
        .map_err(DbOpError::Other)?
        .rows_affected();
        if affected != 1 {
            // Check for equality with pre-existing
            reord::point().await;
            let affected = sqlx::query(
                "
                    SELECT snapshot_id FROM snapshots
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
            .execute(&mut *t)
            .await
            .with_context(|| {
                format!("checking pre-existing snapshot for {created_at:?} is the same")
            })
            .map_err(DbOpError::Other)?
            .rows_affected();
            if affected != 1 {
                return Err(DbOpError::Other(anyhow!(
                    "Snapshot {created_at:?} already existed with a different value set"
                )));
            }
            reord::point().await;
            return Ok(());
        }

        // We just inserted. Check that no event existed at this id
        reord::point().await;
        let affected = sqlx::query("SELECT event_id FROM events WHERE event_id = $1")
            .bind(created_at)
            .execute(&mut *t)
            .await
            .with_context(|| format!("checking that no event existed with this id yet"))
            .map_err(DbOpError::Other)?
            .rows_affected();
        if affected != 0 {
            reord::point().await;
            return Err(DbOpError::Other(anyhow!(
                "Snapshot {created_at:?} has an ulid conflict with a pre-existing event"
            )));
        }

        // TODO fill binary tables
        for binary_id in object.required_binaries() {
            reord::point().await;
            sqlx::query("INSERT INTO snapshots_binaries VALUES ($1, $2)")
                .bind(created_at)
                .bind(binary_id)
                .execute(&mut *t)
                .await
                .with_context(|| format!("marking {created_at:?} as using {binary_id:?}"))
                .map_err(DbOpError::Other)?;
        }

        reord::point().await;
        t.commit()
            .await
            .with_context(|| format!("committing transaction that created {object_id:?}"))
            .map_err(DbOpError::Other)?;
        reord::point().await;
        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> Result<(), DbOpError> {
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
