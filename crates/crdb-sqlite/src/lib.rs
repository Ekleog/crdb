use anyhow::Context;
use crdb_core::{
    normalizer_version, BinPtr, ClientSideDb, ClientStorageInfo, CrdbSyncFn, Db, EventId,
    Importance, LoginInfo, Object, ObjectId, Query, QueryId, ResultExt, SavedObjectMeta,
    SavedQuery, TypeId, Updatedness, Upload, UploadId,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

// TODO(sqlite-high): store Importance and importance_from_queries

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
}

#[allow(unused_variables)] // TODO(sqlite-high): remove
impl Db for SqliteDb {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        importance: Importance,
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
            "INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6, $7, $8, $9)
                         ON CONFLICT DO NOTHING",
        )
        .bind(created_at)
        .bind(type_id)
        .bind(object_id)
        .bind(normalizer_version())
        .bind(snapshot_version)
        .bind(object_json)
        .bind(updatedness)
        .bind(importance.bits())
        .bind(Importance::NONE.bits())
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
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn get_latest<T: Object>(
        &self,
        object_id: ObjectId,
        importance: Importance,
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

    async fn assert_invariants_generic(&self) {
        unimplemented!()
    }

    async fn assert_invariants_for<T: Object>(&self) {
        unimplemented!()
    }
}

#[allow(unused_variables)] // TODO(sqlite-high)
impl ClientSideDb for SqliteDb {
    async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn save_login(&self, _info: LoginInfo) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn get_saved_login(&self) -> crate::Result<Option<LoginInfo>> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn remove_everything(&self) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn get_json(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> crate::Result<serde_json::Value> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        unimplemented!() // TODO(sqlite-high): implement
    }

    async fn client_query(
        &self,
        _type_id: TypeId,
        _query: Arc<Query>,
    ) -> crate::Result<Vec<ObjectId>> {
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

    async fn set_object_importance(
        &self,
        object_id: ObjectId,
        new_importance: Importance,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn set_importance_from_queries(
        &self,
        object_id: ObjectId,
        new_importance_from_queries: Importance,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn client_vacuum(
        &self,
        _notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        _notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
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

    async fn get_saved_objects(&self) -> crate::Result<HashMap<ObjectId, SavedObjectMeta>> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn get_saved_queries(&self) -> crate::Result<HashMap<QueryId, SavedQuery>> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn record_query(
        &self,
        _query_id: QueryId,
        _query: Arc<Query>,
        _type_id: TypeId,
        _importance: Importance,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn set_query_importance(
        &self,
        query_id: QueryId,
        importance: Importance,
        objects_matching_query: Vec<ObjectId>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn forget_query(
        &self,
        _query_id: QueryId,
        _objects_to_unlock: Vec<ObjectId>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }

    async fn update_queries(
        &self,
        _queries: &HashSet<QueryId>,
        _now_have_all_until: Updatedness,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(sqlite-high)
    }
}
