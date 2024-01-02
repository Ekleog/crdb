use super::{Session, SessionRef, SessionToken};
use crate::{
    api::{parse_snapshot, query::Bind},
    db_trait::{
        Db, DbOpError, DynNewEvent, DynNewObject, DynNewRecreation, EventId, ObjectId, Timestamp,
        TypeId,
    },
    full_object::{Change, FullObject},
    BinPtr, CanDoCallbacks, Event, Object, Query, User,
};
use anyhow::{anyhow, Context};
use futures::{Stream, StreamExt};
use sqlx::Row;
use std::{collections::BTreeMap, sync::Arc};

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

    async fn new_recreations(&self) -> impl Send + Stream<Item = DynNewRecreation> {
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        unimplemented!("unsubscribing from a postgresql db does not make sense")
    }

    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> Result<(), DbOpError> {
        let mut t = self
            .db
            .begin()
            .await
            .context("acquiring postgresql transaction")
            .map_err(DbOpError::Other)?;

        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = TypeId(*T::type_ulid());
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        let users_who_can_read = object
            .users_who_can_read(cb)
            .await
            .with_context(|| format!("listing users who can read object {object_id:?}"))
            .map_err(DbOpError::Other)?;
        let is_heavy = object.is_heavy();
        let required_binaries = object.required_binaries();
        let affected =
            sqlx::query("INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING")
                .bind(created_at)
                .bind(type_id)
                .bind(object_id)
                .bind(snapshot_version)
                .bind(object_json)
                .bind(users_who_can_read)
                .bind(is_heavy)
                .bind(&required_binaries)
                .execute(&mut *t)
                .await
                .with_context(|| format!("inserting snapshot {created_at:?}"))
                .map_err(DbOpError::Other)?
                .rows_affected();
        if affected != 1 {
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
            return Ok(());
        }

        // Check that all required binaries are present, always as the last lock obtained in the transaction
        check_required_binaries(&mut t, required_binaries)
            .await
            .map_err(|e| {
                e.with_context(|| {
                    format!(
                        "checking that all binaries for object {object_id:?} are already present"
                    )
                })
            })?;

        t.commit()
            .await
            .with_context(|| format!("committing transaction that created {object_id:?}"))
            .map_err(DbOpError::Other)?;
        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> anyhow::Result<()> {
        let mut transaction = self
            .db
            .begin()
            .await
            .context("acquiring postgresql transaction")
            .map_err(DbOpError::Other)?;

        // Lock the object
        let creation_snapshot = sqlx::query!(
            "SELECT snapshot_id FROM snapshots WHERE object_id = $1 AND is_creation FOR UPDATE",
            object_id.to_uuid(),
        )
        .fetch_optional(&mut *transaction)
        .await
        .with_context(|| format!("locking object {object_id:?} in database"))
        .map_err(DbOpError::Other)?;
        match creation_snapshot {
            None => anyhow::bail!("Object {object_id:?} does not exist yet in database"),
            Some(s) => anyhow::ensure!(
                s.snapshot_id < event_id.to_uuid(),
                "Event {event_id:?} is being submitted before object creation time {:?}",
                s.snapshot_id
            ),
        }

        // Insert the event itself
        let event_json = sqlx::types::Json(&event);
        let required_binaries = event.required_binaries();
        let affected =
            sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
                .bind(event_id)
                .bind(object_id)
                .bind(event_json)
                .bind(required_binaries)
                .execute(&mut *transaction)
                .await
                .with_context(|| format!("inserting event {event_id:?} in database"))
                .map_err(DbOpError::Other)?
                .rows_affected();
        if affected != 1 {
            // Check for equality with pre-existing
            let affected = sqlx::query(
                "
                    SELECT 1 FROM events
                    WHERE event_id = $1
                    AND object_id = $2
                    AND data = $3
                ",
            )
            .bind(event_id)
            .bind(object_id)
            .bind(event_json)
            .execute(&self.db)
            .await
            .with_context(|| format!("checking pre-existing snapshot for {event_id:?} is the same"))
            .map_err(DbOpError::Other)?
            .rows_affected();
            anyhow::ensure!(
                affected == 1,
                "Event {event_id:?} already existed with a different value set"
            );
            // Nothing else to do, event was already inserted
            return Ok(());
        }

        // Clear all snapshots after the event
        sqlx::query("DELETE FROM snapshots WHERE object_id = $1 AND snapshot_id > $2")
            .bind(object_id)
            .bind(event_id)
            .execute(&mut *transaction)
            .await
            .with_context(|| {
                format!("clearing all snapshots for object {object_id:?} after event {event_id:?}")
            })
            .map_err(DbOpError::Other)?;

        // Find the last snapshot for the object
        let last_snapshot = sqlx::query!(
            "
                SELECT snapshot_id, is_latest, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                ORDER BY snapshot_id DESC
                LIMIT 1
            ",
            object_id.to_uuid(),
        )
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("fetching the last snapshot for object {object_id:?}"))?;
        let mut object =
            parse_snapshot::<T>(last_snapshot.snapshot_version, last_snapshot.snapshot)
                .with_context(|| format!("parsing last snapshot for object {object_id:?}"))
                .map_err(DbOpError::Other)?;

        // Remove the "latest snapshot" flag for the object
        // Note that this can be a no-op if the latest snapshot was already deleted above
        sqlx::query("UPDATE snapshots SET is_latest = FALSE WHERE object_id = $1 AND is_latest")
            .bind(object_id)
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("removing latest-snapshot flag for object {object_id:?}"))
            .map_err(DbOpError::Other)?;

        // Apply all events between the last snapshot and the current event
        if !last_snapshot.is_latest {
            let mut events_between_last_snapshot_and_current_event = sqlx::query!(
                "
                    SELECT event_id, data
                    FROM events
                    WHERE object_id = $1
                    AND event_id > $2
                    AND event_id < $3
                    ORDER BY event_id ASC
                ",
                object_id.to_uuid(),
                last_snapshot.snapshot_id,
                event_id.to_uuid(),
            )
            .fetch(&mut *transaction);
            while let Some(e) = events_between_last_snapshot_and_current_event.next().await {
                let e = e
                    .with_context(|| {
                        format!(
                            "fetching all events for {object_id:?} betwen {:?} and {event_id:?}",
                            last_snapshot.snapshot_id
                        )
                    })
                    .map_err(DbOpError::Other)?;
                let e = serde_json::from_value::<T::Event>(e.data)
                    .with_context(|| {
                        format!(
                            "parsing event {:?} of type {:?}",
                            e.event_id,
                            T::type_ulid()
                        )
                    })
                    .map_err(DbOpError::Other)?;

                object.apply(&e);
            }
        }

        // Add the current event to the last snapshot
        object.apply(&event);

        // Save the new snapshot (the new event was already saved above)
        let users_who_can_read = object
            .users_who_can_read(cb)
            .await
            .with_context(|| {
                format!(
                    "listing users who can read for snapshot {event_id:?} of object {object_id:?}"
                )
            })
            .map_err(DbOpError::Other)?;
        sqlx::query("INSERT INTO snapshots VALUES ($1, $2, $3, FALSE, $4, $5, $6, $7, $8, $9)")
            .bind(event_id)
            .bind(TypeId(*T::type_ulid()))
            .bind(object_id)
            .bind(last_snapshot.is_latest)
            .bind(T::snapshot_version())
            .bind(sqlx::types::Json(&object))
            .bind(users_who_can_read)
            .bind(object.is_heavy())
            .bind(object.required_binaries())
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("inserting event {event_id:?} into table"))
            .map_err(DbOpError::Other)?;

        // If needed, re-compute the last snapshot
        if !last_snapshot.is_latest {
            // List all the events since the inserted event
            let mut events_since_inserted = sqlx::query!(
                "
                    SELECT event_id, data
                    FROM events
                    WHERE object_id = $1
                    AND event_id > $2
                    ORDER BY event_id ASC
                ",
                object_id.to_uuid(),
                event_id.to_uuid(),
            )
            .fetch(&mut *transaction);
            let mut last_event_id = None;
            while let Some(e) = events_since_inserted.next().await {
                let e = e
                    .with_context(|| {
                        format!("fetching all events for {object_id:?} after {event_id:?}")
                    })
                    .map_err(DbOpError::Other)?;
                last_event_id = Some(e.event_id);
                let e = serde_json::from_value::<T::Event>(e.data)
                    .with_context(|| {
                        format!(
                            "parsing event {:?} of type {:?}",
                            e.event_id,
                            T::type_ulid()
                        )
                    })
                    .map_err(DbOpError::Other)?;

                object.apply(&e);
            }
            std::mem::drop(events_since_inserted);

            // Save the latest snapshot
            let users_who_can_read = object.users_who_can_read(cb).await.with_context(|| {
                format!(
                    "listing users who can read for snapshot {event_id:?} of object {object_id:?}"
                )
            })?;
            sqlx::query(
                "INSERT INTO snapshots VALUES ($1, $2, $3, FALSE, TRUE, $4, $5, $6, $7, $8)",
            )
            .bind(
                last_event_id
                    .expect("Entered the 'recomputing last snapshot' stage without any new events"),
            )
            .bind(TypeId(*T::type_ulid()))
            .bind(object_id)
            .bind(T::snapshot_version())
            .bind(sqlx::types::Json(&object))
            .bind(users_who_can_read)
            .bind(object.is_heavy())
            .bind(object.required_binaries())
            .execute(&mut *transaction)
            .await
            .with_context(|| format!("inserting event {event_id:?} into table"))
            .map_err(DbOpError::Other)?;
        }

        // Check that all required binaries are present, always as the last lock obtained in the transaction
        check_required_binaries(&mut transaction, event.required_binaries())
            .await
            .map_err(|e| {
                e.with_context(|| {
                    format!(
                        "checking that all binaries for object {object_id:?} are already present"
                    )
                })
            })?;

        transaction
            .commit()
            .await
            .with_context(|| {
                format!("committing transaction adding event {event_id:?} to object {object_id:?}")
            })
            .map_err(DbOpError::Other)?;

        Ok(())
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        let mut transaction = self
            .db
            .begin()
            .await
            .context("acquiring postgresql transaction")?;

        // Atomically perform all the reads here
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .context("setting transaction as repeatable read")?;

        get_impl::<T>(&mut *transaction, ptr).await
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
        let mut transaction = self
            .db
            .begin()
            .await
            .context("acquiring postgresql transaction")?;

        // Atomically perform all the reads here
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .context("setting transaction as repeatable read")?;

        let query = format!(
            "SELECT object_id FROM snapshots WHERE is_latest AND type_id = $1 AND ({})",
            q.where_clause(2)
        );
        let mut query = sqlx::query(&query);
        for b in q.binds() {
            match b {
                Bind::Json(v) => query = query.bind(v),
                Bind::Str(v) => query = query.bind(v),
                Bind::F64(v) => query = query.bind(v),
                Bind::I64(v) => query = query.bind(v),
            }
        }
        let ids = query
            .fetch_all(&mut *transaction)
            .await
            .with_context(|| format!("listing objects matching query {q:?}"))?;

        Ok(async_stream::stream! {
            for id in ids {
                let object = get_impl::<T>(&mut *transaction, ObjectId::from_uuid(id.get(0))).await;
                match object {
                    Err(e) => yield Err(e),
                    Ok(None) => panic!("Found object that matches query, but was unable to get it"),
                    Ok(Some(o)) => yield Ok(o),
                }
            }
        })
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

async fn check_required_binaries(
    t: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mut binaries: Vec<BinPtr>,
) -> Result<(), DbOpError> {
    // FOR KEY SHARE: prevent DELETE of the binaries while `t` is running
    let present_ids = sqlx::query("SELECT id FROM binaries WHERE id = ANY ($1) FOR KEY SHARE")
        .bind(&binaries)
        .fetch_all(&mut **t)
        .await
        .context("listing binaries already present in database")
        .map_err(DbOpError::Other)?;
    binaries.retain(|b| {
        present_ids
            .iter()
            .any(|i| i.get::<uuid::Uuid, _>(0) == b.to_uuid())
    });
    if !binaries.is_empty() {
        return Err(DbOpError::MissingBinPtrs(binaries));
    }
    Ok(())
}

/// Note: this assumes that `transaction` is set to REPEATABLE READ for consistency
async fn get_impl<T: Object>(
    transaction: &mut sqlx::PgConnection,
    ptr: ObjectId,
) -> anyhow::Result<Option<FullObject>> {
    const EVENT_ID: usize = 0;
    const EVENT_DATA: usize = 1;

    let creation_snapshot = sqlx::query!(
        "
            SELECT snapshot_id, snapshot_version, snapshot
            FROM snapshots
            WHERE object_id = $1
            AND type_id = $2
            AND is_creation
        ",
        ptr.to_uuid(),
        uuid::Uuid::from_bytes(T::type_ulid().to_bytes()),
    )
    .fetch_optional(&mut *transaction)
    .await
    .with_context(|| format!("fetching creation snapshot for object {ptr:?}"))?;
    let creation_snapshot = match creation_snapshot {
        Some(s) => s,
        None => return Ok(None),
    };

    let events =
        sqlx::query("SELECT event_id, data FROM events WHERE object_id = $1 ORDER BY event_id")
            .bind(ptr)
            .fetch_all(&mut *transaction)
            .await
            .with_context(|| format!("fetching all events for object {ptr:?}"))?;

    let latest_snapshot = sqlx::query!(
        "
            SELECT snapshot_id, snapshot_version, snapshot
            FROM snapshots
            WHERE object_id = $1
            AND type_id = $2
            AND is_latest
        ",
        ptr.to_uuid(),
        uuid::Uuid::from_bytes(T::type_ulid().to_bytes()),
    )
    .fetch_one(&mut *transaction)
    .await
    .with_context(|| format!("fetching latest snapshot for object {ptr:?}"))?;

    // Build the FullObject from the parts
    let creation = Arc::new(
        parse_snapshot::<T>(
            creation_snapshot.snapshot_version,
            creation_snapshot.snapshot,
        )
        .with_context(|| {
            format!(
                "parsing snapshot {:?} as type {:?}",
                creation_snapshot.snapshot_id,
                T::type_ulid()
            )
        })?,
    );
    let first_event = events
        .partition_point(|e| e.get::<uuid::Uuid, _>(EVENT_ID) <= creation_snapshot.snapshot_id);
    let after_last_event =
        events.partition_point(|e| e.get::<uuid::Uuid, _>(EVENT_ID) <= latest_snapshot.snapshot_id);
    let mut changes = BTreeMap::new();
    for e in events
        .into_iter()
        .skip(first_event)
        .take(after_last_event - first_event)
    {
        let event_id = EventId::from_uuid(e.get(EVENT_ID));
        changes.insert(
            event_id,
            Change::new(Arc::new(
                serde_json::from_value::<T::Event>(e.get(EVENT_DATA)).with_context(|| {
                    format!("parsing event {event_id:?} as type {:?}", T::type_ulid())
                })?,
            )),
        );
    }
    if let Some(mut c) = changes.last_entry() {
        c.get_mut().set_snapshot(Arc::new(
            parse_snapshot::<T>(latest_snapshot.snapshot_version, latest_snapshot.snapshot)
                .with_context(|| {
                    format!(
                        "parsing snapshot {:?} as type {:?}",
                        latest_snapshot.snapshot_id,
                        T::type_ulid()
                    )
                })?,
        ));
    } else {
        assert!(
            creation_snapshot.snapshot_id == latest_snapshot.snapshot_id,
            "got no events but latest_snapshot {:?} != creation_snapshot {:?}",
            latest_snapshot.snapshot_id,
            creation_snapshot.snapshot_id
        );
    }

    Ok(Some(FullObject::from_parts(
        ptr,
        EventId::from_uuid(creation_snapshot.snapshot_id),
        creation,
        changes,
    )))
}
