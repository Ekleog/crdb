use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    error::ResultExt,
    full_object::FullObject,
    BinPtr, CanDoCallbacks, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use anyhow::Context;
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
    async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
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
    ) -> crate::Result<()> {
        reord::point().await;
        let mut t = self
            .db
            .begin()
            .await
            .wrap_context("acquiring sqlite transaction")?;

        // TODO add reord lock over whole database

        reord::point().await;
        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = *T::type_ulid();
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        reord::point().await;
        // TODO: make it possible to atomically create-and-lock an object
        let affected = sqlx::query(
            "INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, FALSE, FALSE)
                         ON CONFLICT DO NOTHING",
        )
        .bind(created_at)
        .bind(type_id)
        .bind(object_id)
        .bind(snapshot_version)
        .bind(object_json)
        .execute(&mut *t)
        .await
        .wrap_with_context(|| format!("inserting snapshot {created_at:?}"))?
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
            .fetch_all(&mut *t)
            .await
            .wrap_with_context(|| {
                format!("checking pre-existing snapshot for {created_at:?} is the same")
            })?
            .len();
            if affected != 1 {
                return Err(crate::Error::EventAlreadyExists(created_at));
            }
            reord::point().await;
            return Ok(());
        }

        // We just inserted. Check that no event existed at this id
        reord::point().await;
        let affected = sqlx::query("SELECT event_id FROM events WHERE event_id = $1")
            .bind(created_at)
            .fetch_all(&mut *t)
            .await
            .wrap_with_context(|| format!("checking that no event existed with this id yet"))?
            .len();
        if affected != 0 {
            reord::point().await;
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        for binary_id in object.required_binaries() {
            reord::point().await;
            sqlx::query("INSERT INTO snapshots_binaries VALUES ($1, $2)")
                .bind(created_at)
                .bind(binary_id)
                .execute(&mut *t)
                .await
                .wrap_with_context(|| format!("marking {created_at:?} as using {binary_id:?}"))?;
        }

        reord::point().await;
        t.commit()
            .await
            .wrap_with_context(|| format!("committing transaction that created {object_id:?}"))?;
        reord::point().await;
        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
