use crate::{
    api::{UploadId, UploadOrBinPtr},
    db_trait::Db,
    error::ResultExt,
    fts, BinPtr, CanDoCallbacks, EventId, Object, ObjectId, Query,
};
use anyhow::Context;
use std::sync::Arc;

use super::ClientStorageInfo;

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

    pub async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        unimplemented!() // TODO(sqlite)
    }

    pub async fn query<T: Object>(&self, _q: &Query) -> crate::Result<Vec<ObjectId>> {
        unimplemented!() // TODO(sqlite): implement
    }

    pub async fn unlock(&self, _object_id: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(test)
    }

    pub async fn vacuum(&self) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite)
    }

    pub async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        unimplemented!() // TODO(sqlite)
    }

    pub async fn get_upload(&self, _upload_id: UploadId) -> crate::Result<UploadOrBinPtr> {
        unimplemented!() // TODO(sqlite)
    }

    pub async fn enqueue_upload(
        &self,
        _upload: UploadOrBinPtr,
        _required_binaries: Vec<BinPtr>,
    ) -> crate::Result<UploadId> {
        unimplemented!() // TODO(sqlite)
    }

    pub async fn upload_finished(&self, _upload_id: UploadId) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite)
    }
}

#[allow(unused_variables)] // TODO(sqlite): remove
impl Db for SqliteDb {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        reord::point().await;
        let mut t = self
            .db
            .begin()
            .await
            .wrap_context("acquiring sqlite transaction")?;

        // TODO(sqlite): add reord lock over whole database

        reord::point().await;
        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = *T::type_ulid();
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        reord::point().await;
        let affected = sqlx::query(
            "INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6, $7)
                         ON CONFLICT DO NOTHING",
        )
        .bind(created_at)
        .bind(type_id)
        .bind(object_id)
        .bind(&fts::normalizer_version())
        .bind(snapshot_version)
        .bind(object_json)
        .bind(lock)
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
            return Ok(None);
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
        Ok(Some(object))
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite): implement
    }

    async fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        unimplemented!() // TODO(sqlite): implement
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        force_lock: bool,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite): implement
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite): implement
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite): implement
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
        unimplemented!() // TODO(sqlite): implement
    }
}
