use anyhow::Context;
use crdb_core::{
    normalizer_version, BinPtr, ClientSideDb, ClientStorageInfo, Db, EventId, Lock, LoginInfo,
    Object, ObjectId, Query, QueryId, ResultExt, TypeId, Updatedness, Upload, UploadId,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[cfg(test)]
mod tests;

pub use crdb_core::{Error, Result};

pub struct SqliteDb {
    db: sqlx::SqlitePool,
}

impl SqliteDb {
    pub async fn connect_impl(db: sqlx::SqlitePool) -> anyhow::Result<SqliteDb> {
        sqlx::migrate!("./migrations")
            .run(&db)
            .await
            .context("running migrations on sqlite database")?;
        Ok(SqliteDb { db })
    }

    pub async fn connect(url: &str) -> anyhow::Result<SqliteDb> {
        Self::connect_impl(sqlx::SqlitePool::connect(url).await?).await
    }

    pub async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn save_login(&self, _info: LoginInfo) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn get_saved_login(&self) -> crate::Result<Option<LoginInfo>> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn query<T: Object>(&self, _query: Arc<Query>) -> crate::Result<Vec<ObjectId>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    pub async fn vacuum(
        &self,
        mut _notify_removals: impl 'static + FnMut(ObjectId),
        mut _notify_query_removals: impl 'static + FnMut(QueryId),
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn remove_everything(&self) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn get_subscribed_objects(
        &self,
    ) -> crate::Result<HashMap<ObjectId, (TypeId, serde_json::Value, Option<Updatedness>)>> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn get_subscribed_queries(
        &self,
    ) -> crate::Result<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn subscribe_query(
        &self,
        _query_id: QueryId,
        _query: Arc<Query>,
        _type_id: TypeId,
        _lock: bool,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn unsubscribe_query(
        &self,
        _query_id: QueryId,
        _objects_to_unlock: Vec<ObjectId>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    pub async fn update_queries(
        &self,
        _queries: &HashSet<QueryId>,
        _now_have_all_until: Updatedness,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }
}

#[allow(unused_variables)] // TODO(sqlite-high): remove
impl Db for SqliteDb {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut t = self
            .db
            .begin()
            .await
            .wrap_context("acquiring sqlite transaction")?;

        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = *T::type_ulid();
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        let affected = sqlx::query(
            "INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6, $7, $8)
                         ON CONFLICT DO NOTHING",
        )
        .bind(created_at)
        .bind(type_id)
        .bind(object_id)
        .bind(normalizer_version())
        .bind(snapshot_version)
        .bind(object_json)
        .bind(updatedness)
        .bind(lock.bits())
        .execute(&mut *t)
        .await
        .wrap_with_context(|| format!("inserting snapshot {created_at:?}"))?
        .rows_affected();
        if affected != 1 {
            // Check for equality with pre-existing
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
            return Ok(None);
        }

        // We just inserted. Check that no event existed at this id
        let affected = sqlx::query("SELECT event_id FROM events WHERE event_id = $1")
            .bind(created_at)
            .fetch_all(&mut *t)
            .await
            .wrap_context("checking that no event existed with this id yet")?
            .len();
        if affected != 0 {
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        for binary_id in object.required_binaries() {
            sqlx::query("INSERT INTO snapshots_binaries VALUES ($1, $2)")
                .bind(created_at)
                .bind(binary_id)
                .execute(&mut *t)
                .await
                .wrap_with_context(|| format!("marking {created_at:?} as using {binary_id:?}"))?;
        }

        t.commit()
            .await
            .wrap_with_context(|| format!("committing transaction that created {object_id:?}"))?;
        Ok(Some(object))
    }

    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn get_latest<T: Object>(
        &self,
        lock: Lock,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    /// Returns the number of errors that happened while re-encoding
    async fn reencode_old_versions<T: Object>(&self) -> usize {
        unimplemented!() // TODO(sqlite-high)
    }
}

#[allow(unused_variables)] // TODO(sqlite-high)
impl ClientSideDb for SqliteDb {
    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn remove_event<T: Object>(
        &self,
        _object_id: ObjectId,
        _event_id: EventId,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn change_locks(
        &self,
        _unlock: Lock,
        _then_lock: Lock,
        _object_id: ObjectId,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn get_upload(&self, _upload_id: UploadId) -> crate::Result<Option<Upload>> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn enqueue_upload(
        &self,
        _upload: Upload,
        _required_binaries: Vec<BinPtr>,
    ) -> crate::Result<UploadId> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn upload_finished(&self, _upload_id: UploadId) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }
}
