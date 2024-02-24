use anyhow::{anyhow, Context};
use crdb_cache::CacheDb;
use crdb_core::{
    normalizer_version, BinPtr, CanDoCallbacks, Db, DbPtr, Event, EventId, Lock, Object,
    ObjectData, ObjectId, Query, ResultExt, Session, SessionRef, SessionToken, SnapshotData,
    SystemTimeExt, TypeId, Update, UpdateData, Updatedness, User,
};
use crdb_core::{ComboLock, Decimal, JsonPathItem, ReadPermsChanges, ServerSideDb};
use crdb_helpers::parse_snapshot;
use futures::{future::Either, StreamExt, TryStreamExt};
use lockable::{LockPool, Lockable};
use sqlx::Row;
use std::{
    collections::{hash_map, BTreeMap, HashMap, HashSet},
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Weak},
    time::SystemTime,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests;

pub use crdb_core::{Error, Result};

pub struct PostgresDb<Config: crdb_core::Config> {
    db: sqlx::PgPool,
    cache_db: Weak<CacheDb<PostgresDb<Config>>>,
    event_locks: LockPool<EventId>,
    // TODO(perf-high): make into a RwLockPool, which locks the snapshot fields
    // create and submit take a write(), get and update_users_who_can_read
    // take a read(). This should significantly improve performance
    object_locks: LockPool<ObjectId>,
    _phantom: PhantomData<Config>,
}

impl<Config: crdb_core::Config> PostgresDb<Config> {
    pub async fn connect(
        db: sqlx::PgPool,
        cache_watermark: usize,
    ) -> anyhow::Result<(Arc<PostgresDb<Config>>, Arc<CacheDb<PostgresDb<Config>>>)> {
        sqlx::migrate!("./migrations")
            .run(&db)
            .await
            .context("running migrations on postgresql database")?;
        let mut postgres_db = None;
        let cache_db = Arc::new_cyclic(|cache_db| {
            let db = Arc::new(PostgresDb {
                db,
                cache_db: cache_db.clone(),
                event_locks: LockPool::new(),
                object_locks: LockPool::new(),
                _phantom: PhantomData,
            });
            postgres_db = Some(db.clone());
            CacheDb::new(db, cache_watermark)
        });
        Ok((postgres_db.unwrap(), cache_db))
    }

    pub async fn login_session(
        &self,
        session: Session,
    ) -> crate::Result<(SessionToken, SessionRef)> {
        let token = SessionToken::new();
        sqlx::query("INSERT INTO sessions VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(token)
            .bind(session.session_ref)
            .bind(session.user_id)
            .bind(&session.session_name)
            .bind(session.login_time.ms_since_posix()?)
            .bind(session.last_active.ms_since_posix()?)
            .bind(
                session
                    .expiration_time
                    .map(|t| t.ms_since_posix())
                    .transpose()?,
            )
            .execute(&self.db)
            .await
            .wrap_with_context(|| {
                format!("logging in new session {token:?} with data {session:?}")
            })?;
        Ok((token, session.session_ref))
    }

    pub async fn resume_session(&self, token: SessionToken) -> crate::Result<Session> {
        // TODO(server-high): actually implement expiration_time validation
        let res = sqlx::query!(
            "SELECT * FROM sessions WHERE session_token = $1",
            token as SessionToken
        )
        .fetch_optional(&self.db)
        .await
        .wrap_with_context(|| format!("resuming session for {token:?}"))?;
        let Some(res) = res else {
            return Err(crate::Error::InvalidToken(token));
        };
        Ok(Session {
            user_id: User::from_uuid(res.user_id),
            session_ref: SessionRef::from_uuid(res.session_ref),
            session_name: res.name,
            login_time: SystemTime::from_ms_since_posix(res.login_time)
                .expect("negative timestamp made its way into database"),
            last_active: SystemTime::from_ms_since_posix(res.last_active)
                .expect("negative timestamp made its way into database"),
            expiration_time: res
                .expiration_time
                .map(SystemTime::from_ms_since_posix)
                .transpose()
                .expect("negative timestamp made its way into database"),
        })
    }

    pub async fn mark_session_active(
        &self,
        token: SessionToken,
        at: SystemTime,
    ) -> crate::Result<()> {
        let affected = sqlx::query("UPDATE sessions SET last_active = $1 WHERE session_token = $2")
            .bind(at.ms_since_posix()?)
            .bind(token)
            .execute(&self.db)
            .await
            .wrap_with_context(|| format!("marking session {token:?} as active as of {at:?}"))?
            .rows_affected();
        if affected != 1 {
            return Err(crate::Error::InvalidToken(token));
        }
        Ok(())
    }

    pub async fn rename_session(&self, token: SessionToken, new_name: &str) -> crate::Result<()> {
        let affected = sqlx::query("UPDATE sessions SET name = $1 WHERE session_token = $2")
            .bind(new_name)
            .bind(token)
            .execute(&self.db)
            .await
            .wrap_with_context(|| format!("renaming session {token:?} into {new_name:?}"))?
            .rows_affected();
        if affected != 1 {
            return Err(crate::Error::InvalidToken(token));
        }
        Ok(())
    }

    pub async fn list_sessions(&self, user: User) -> crate::Result<Vec<Session>> {
        let rows = sqlx::query!("SELECT * FROM sessions WHERE user_id = $1", user as User)
            .fetch_all(&self.db)
            .await
            .wrap_with_context(|| format!("listing sessions for {user:?}"))?;
        let sessions = rows
            .into_iter()
            .map(|r| Session {
                user_id: User::from_uuid(r.user_id),
                session_ref: SessionRef::from_uuid(r.session_ref),
                session_name: r.name,
                login_time: SystemTime::from_ms_since_posix(r.login_time)
                    .expect("negative timestamp made its way into database"),
                last_active: SystemTime::from_ms_since_posix(r.last_active)
                    .expect("negative timestamp made its way into database"),
                expiration_time: r
                    .expiration_time
                    .map(SystemTime::from_ms_since_posix)
                    .transpose()
                    .expect("negative timestamp made its way into database"),
            })
            .collect();
        Ok(sessions)
    }

    pub async fn disconnect_session(&self, user: User, session: SessionRef) -> crate::Result<()> {
        sqlx::query("DELETE FROM sessions WHERE user_id = $1 AND session_ref = $2")
            .bind(user)
            .bind(session)
            .execute(&self.db)
            .await
            .wrap_with_context(|| format!("disconnecting session {session:?}"))?;
        // If nothing to delete it's fine, the session was probably already disconnected
        Ok(())
    }

    async fn reencode<T: Object>(
        &self,
        object_id: ObjectId,
        snapshot_id: EventId,
    ) -> crate::Result<()> {
        // First, take a lock on the object, so that no new event comes in during
        // execution that would be overwritten by this select-then-update
        let _lock = self.object_locks.async_lock(object_id).await;

        // Then, read the snapshot, knowing it can't change under our feet
        let Some(s) = sqlx::query!(
            "
                SELECT normalizer_version, snapshot_version, snapshot
                FROM snapshots
                WHERE snapshot_id = $1
            ",
            snapshot_id as EventId,
        )
        .fetch_optional(&self.db)
        .await
        .wrap_with_context(|| format!("fetching snapshot {snapshot_id:?}"))?
        else {
            // No such snapshot. It was certainly deleted by a concurrent vacuum or similar. Ignore.
            return Ok(());
        };

        // If it is already up-to-date (eg. has been rewritten by an event coming in), ignore
        if s.normalizer_version >= normalizer_version()
            && s.snapshot_version >= T::snapshot_version()
        {
            return Ok(());
        }

        // If not, we still need to parse-and-reencode it
        let snapshot = parse_snapshot::<T>(s.snapshot_version, s.snapshot)
            .wrap_with_context(|| format!("parsing snapshot {snapshot_id:?}"))?;
        sqlx::query(
            "
                UPDATE snapshots
                SET snapshot = $1, snapshot_version = $2, normalizer_version = $3
                WHERE snapshot_id = $4
            ",
        )
        .bind(sqlx::types::Json(&snapshot))
        .bind(T::snapshot_version())
        .bind(normalizer_version())
        .bind(snapshot_id)
        .execute(&self.db)
        .await
        .wrap_with_context(|| format!("re-encoding snapshot data for {snapshot_id:?}"))?;

        Ok(())
    }

    /// Cleans up and optimizes up the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    pub async fn vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<SystemTime>,
        mut notify_recreation: impl FnMut(Update, HashSet<User>),
    ) -> crate::Result<()> {
        // TODO(perf-high): do not vacuum away binaries that have been uploaded less than an hour ago
        if let Some(t) = kill_sessions_older_than {
            // Discard all sessions that were last active too long ago
            reord::point().await;
            sqlx::query!(
                "DELETE FROM sessions WHERE last_active < $1",
                t.ms_since_posix()?,
            )
            .execute(&self.db)
            .await
            .wrap_context("cleaning up old sessions")?;
        }

        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::vacuum after CacheDb went away");

        {
            // Discard all unrequired snapshots, as well as unused fields of creation snapshots
            // In addition, auto-recreate the objects that need re-creation
            reord::point().await;
            let mut objects = sqlx::query!(
                "
                    SELECT DISTINCT object_id, type_id
                    FROM snapshots
                    WHERE (NOT (is_creation OR is_latest))
                    OR ((NOT is_latest)
                        AND (users_who_can_read IS NOT NULL
                            OR users_who_can_read_depends_on IS NOT NULL
                            OR reverse_dependents_to_update IS NOT NULL))
                    OR ((NOT is_creation) AND snapshot_id < $1)
                ",
                no_new_changes_before.unwrap_or_else(|| EventId::from_u128(0)) as EventId
            )
            .fetch(&self.db);
            while let Some(row) = objects.next().await {
                let row = row.wrap_context("listing objects with snapshots to cleanup")?;
                let object_id = ObjectId::from_uuid(row.object_id);
                let _lock = reord::Lock::take_named(format!("{object_id:?}")).await;
                let _lock = self.object_locks.async_lock(object_id).await;
                reord::maybe_lock().await;
                sqlx::query(
                    "DELETE FROM snapshots WHERE object_id = $1 AND NOT (is_creation OR is_latest)",
                )
                .bind(object_id)
                .execute(&self.db)
                .await
                .wrap_with_context(|| format!("deleting useless snapshots from {object_id:?}"))?;
                reord::maybe_lock().await;
                sqlx::query(
                    "
                        UPDATE snapshots
                        SET users_who_can_read = NULL,
                            users_who_can_read_depends_on = NULL,
                            reverse_dependents_to_update = NULL
                        WHERE object_id = $1
                        AND NOT is_latest
                    ",
                )
                .bind(object_id)
                .execute(&self.db)
                .await
                .wrap_with_context(|| format!("resetting creation snapshot of {object_id:?}"))?;
                reord::point().await;
                if let Some(event_id) = no_new_changes_before {
                    let type_id = TypeId::from_uuid(row.type_id);
                    reord::point().await;
                    let recreation_result = Config::recreate_no_lock(
                        self,
                        type_id,
                        object_id,
                        event_id,
                        updatedness,
                        &*cache_db,
                    )
                    .await
                    .wrap_with_context(|| {
                        format!("recreating {object_id:?} at time {event_id:?}")
                    })?;
                    if let Some((new_created_at, snapshot_version, data, users_who_can_read)) =
                        recreation_result
                    {
                        reord::point().await;
                        notify_recreation(
                            Update {
                                object_id,
                                data: UpdateData::Creation {
                                    type_id,
                                    created_at: new_created_at,
                                    snapshot_version,
                                    data: Arc::new(data),
                                },
                            },
                            users_who_can_read,
                        );
                    }
                }
            }
        }

        // Get rid of no-longer-referenced binaries
        reord::maybe_lock().await;
        sqlx::query(
            "
                DELETE FROM binaries
                WHERE NOT EXISTS (
                    SELECT 1 FROM snapshots WHERE binary_id = ANY(required_binaries)
                    UNION
                    SELECT 1 FROM events WHERE binary_id = ANY(required_binaries)
                )
            ",
        )
        .execute(&self.db)
        .await
        .wrap_context("deleting no-longer-referenced binaries")?;
        reord::point().await;

        // Finally, take care of the database itself
        // This is very slow for tests and produces deadlock false-positives, plus it's useless there.
        // So let's just skip it in tests.
        #[cfg(not(test))]
        sqlx::query("VACUUM ANALYZE")
            .execute(&self.db)
            .await
            .wrap_context("vacuuming database")?;
        reord::point().await;

        Ok(())
    }

    /// Returns the list of all reverse-dependencies of `object_id`
    async fn get_rdeps<'a, E: sqlx::Executor<'a, Database = sqlx::Postgres>>(
        &self,
        connection: E,
        object_id: ObjectId,
    ) -> anyhow::Result<Vec<ObjectId>> {
        reord::point().await;
        let rdeps = sqlx::query!(
            "
                SELECT object_id
                FROM snapshots
                WHERE $1 = ANY (users_who_can_read_depends_on)
                AND is_latest
                AND object_id != $1
            ",
            object_id as ObjectId,
        )
        .map(|o| ObjectId::from_uuid(o.object_id))
        .fetch_all(connection)
        .await
        .with_context(|| format!("fetching the list of reverse-dependencies for {object_id:?}"))?;
        Ok(rdeps)
    }

    /// Update the list of users who can read `object`
    ///
    /// `_lock` is a lock that makes sure `object` is not being modified while this executes.
    pub async fn update_users_who_can_read<C: CanDoCallbacks>(
        &self,
        requested_by: ObjectId,
        object_id: ObjectId,
        cb: &C,
    ) -> anyhow::Result<ReadPermsChanges> {
        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Take the locks
        let _lock = (
            reord::Lock::take_named(format!("{object_id:?}")).await,
            self.object_locks.async_lock(object_id).await,
        );

        // Retrieve the snapshot
        reord::point().await;
        let res = sqlx::query!(
            "
                SELECT type_id, snapshot_version, snapshot, users_who_can_read FROM snapshots
                WHERE object_id = $1
                AND is_latest
            ",
            object_id as ObjectId,
        )
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("fetching latest snapshot for object {object_id:?}"))?;
        let type_id = TypeId::from_uuid(res.type_id);
        let users_who_can_read_before = res
            .users_who_can_read
            .ok_or_else(|| {
                crate::Error::Other(anyhow!("Latest snapshot had NULL users_who_can_read"))
            })?
            .into_iter()
            .map(User::from_uuid)
            .collect::<HashSet<User>>();

        // Figure out the new value of users_who_can_read
        let (users_who_can_read, users_who_can_read_depends_on, _locks) =
            Config::get_users_who_can_read(
                self,
                object_id,
                type_id,
                res.snapshot_version,
                res.snapshot,
                cb,
            )
            .await
            .with_context(|| format!("updating users_who_can_read cache of {object_id:?}"))?;
        let users_who_can_read_after = users_who_can_read
            .iter()
            .copied()
            .collect::<HashSet<User>>();

        // Save it
        reord::maybe_lock().await;
        let affected = sqlx::query(
            "
                UPDATE snapshots
                SET users_who_can_read = $1,
                    users_who_can_read_depends_on = $2
                WHERE object_id = $3
                AND is_latest
            ",
        )
        .bind(users_who_can_read.into_iter().collect::<Vec<_>>())
        .bind(&users_who_can_read_depends_on)
        .bind(object_id)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!("updating users_who_can_read in latest snapshot for {object_id:?}")
        })?
        .rows_affected();
        reord::point().await;
        anyhow::ensure!(
            affected == 1,
            "Failed to update latest snapshot of users_who_can_read"
        );

        // If needed, take a lock on the requester to update its requested-updates field
        let _lock = if !users_who_can_read_depends_on
            .iter()
            .any(|o| *o == requested_by)
        {
            Some((
                reord::Lock::take_named(format!("{requested_by:?}")),
                self.object_locks.async_lock(requested_by).await,
            ))
        } else {
            None
        };

        // Remove the request to update
        // TODO(misc-low): Consider switching to serializable transactions only and discarding locking. This would
        // remove the requirement on `Object::users_who_can_read` that `.get()` must always be called in the same
        // order. Maybe make serializable transactions a feature of this crate? If not we should at least provide
        // an easy way for users to fuzz their Object implementation for these deadlock conditions
        reord::maybe_lock().await;
        let affected = sqlx::query(
            "
                UPDATE snapshots
                SET reverse_dependents_to_update = array_remove(reverse_dependents_to_update, $1)
                WHERE object_id = $2 AND is_latest
            ",
        )
        .bind(object_id)
        .bind(requested_by)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!(
                "removing {object_id:?} from the list of rev-deps of {requested_by:?} to update"
            )
        })?
        .rows_affected();
        reord::point().await;
        anyhow::ensure!(
            affected == 1,
            "Failed to mark reverse dependent {object_id:?} of {requested_by:?} as updated"
        );

        reord::point().await;
        transaction.commit().await.wrap_with_context(|| {
            format!("committing transaction that updated the rdeps of {object_id:?}")
        })?;
        reord::point().await;

        // TODO(perf-low): there must be some libstd function to compute the two at once?
        // but symmetric_difference doesn't seem to indicate which set the value came from
        let lost_read = users_who_can_read_before
            .difference(&users_who_can_read_after)
            .copied()
            .collect();
        let gained_read = users_who_can_read_after
            .difference(&users_who_can_read_before)
            .copied()
            .collect();
        Ok(ReadPermsChanges {
            object_id,
            type_id,
            lost_read,
            gained_read,
        })
    }

    async fn update_rdeps<C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        cb: &C,
    ) -> anyhow::Result<Vec<ReadPermsChanges>> {
        // This is NOT in the same transaction as creation/submission of the object!
        // The reason is, because CanDoCallbacks will read data outside of the transaction. So it would miss the changes that we brought in
        // with the transaction otherwise.
        // An alternative would be to have CanDoCallbacks run inside the transaction itself; but let's keep the current behavior of lazily
        // recomputing users_who_can_read upon reverse-dependent event submission.

        let rdeps = sqlx::query!(
            r#"
                SELECT UNNEST(reverse_dependents_to_update) AS "rdep!"
                FROM snapshots
                WHERE object_id = $1
                AND reverse_dependents_to_update IS NOT NULL
            "#,
            object_id as ObjectId,
        )
        .map(|r| ObjectId::from_uuid(r.rdep))
        .fetch_all(&self.db)
        .await
        .wrap_with_context(|| {
            format!("listing reverse dependents of {object_id:?} that need updating")
        })?;
        let mut res = Vec::with_capacity(rdeps.len());
        for o in rdeps {
            if o != object_id {
                let changes = self.update_users_who_can_read(object_id, o, cb)
                    .await
                    .with_context(|| format!("updating users_who_can_read field for {o:?} on behalf of {object_id:?}"))?;
                res.push(changes);
            }
        }
        Ok(res)
    }

    pub async fn update_pending_rdeps(&self) -> crate::Result<()> {
        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::create after CacheDb went away");
        let cache_db = &*cache_db;

        let mut rdep_update_res = sqlx::query!(
            "
                SELECT object_id
                FROM snapshots
                WHERE array_length(reverse_dependents_to_update, 1) IS NOT NULL
            "
        )
        .map(|r| ObjectId::from_uuid(r.object_id))
        .fetch(&self.db)
        .map(|object_id| async move {
            let object_id = object_id.wrap_context("listing updates with rdeps to update")?;
            self.update_rdeps(object_id, cache_db)
                .await
                .wrap_with_context(|| format!("updating rdeps of {object_id:?}"))
        })
        .buffered(32);
        while let Some(res) = rdep_update_res.next().await {
            res?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // TODO(misc-low): refactor to have a proper struct
    async fn write_snapshot<'a, T: Object, C: CanDoCallbacks>(
        &'a self,
        transaction: &mut sqlx::PgConnection,
        snapshot_id: EventId,
        object_id: ObjectId,
        is_creation: bool,
        is_latest: bool,
        rdeps: Option<&[ObjectId]>,
        object: &T,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> crate::Result<Vec<ComboLock<'a>>> {
        let (users_who_can_read, users_who_can_read_depends_on, locks) = if is_latest {
            let (a, b, c) = self
                .get_users_who_can_read::<T, _>(object_id, object, cb)
                .await
                .wrap_with_context(|| {
                    format!(
                        "listing users who can read for snapshot {snapshot_id:?} of {object_id:?}"
                    )
                })?;
            (Some(a), Some(b), c)
        } else {
            (None, None, Vec::new())
        };
        assert!(
            !is_latest || rdeps.is_some(),
            "Latest snapshots must always list their reverse dependencies"
        );

        reord::maybe_lock().await;
        let result = sqlx::query(
            "INSERT INTO snapshots VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(snapshot_id)
        .bind(T::type_ulid())
        .bind(object_id)
        .bind(is_creation)
        .bind(is_latest)
        .bind(normalizer_version())
        .bind(T::snapshot_version())
        .bind(sqlx::types::Json(object))
        .bind(users_who_can_read.map(|u| u.into_iter().collect::<Vec<_>>()))
        .bind(users_who_can_read_depends_on)
        .bind(rdeps)
        .bind(object.required_binaries())
        .bind(updatedness)
        .execute(&mut *transaction)
        .await;
        reord::point().await;

        match result {
            Ok(_) => Ok(locks),
            Err(sqlx::Error::Database(err)) if err.constraint() == Some("snapshots_pkey") => {
                Err(crate::Error::EventAlreadyExists(snapshot_id))
            }
            Err(e) => Err(e)
                .wrap_with_context(|| format!("inserting snapshot {snapshot_id:?} into table")),
        }
    }

    async fn create_impl<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Updatedness,
        cb: &C,
    ) -> crate::Result<Either<Arc<T>, Option<EventId>>> {
        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Acquire the locks required to create the object
        let _lock = reord::Lock::take_named(format!("{created_at:?}")).await;
        let _lock = self.event_locks.async_lock(created_at).await;
        let _lock = reord::Lock::take_named(format!("{object_id:?}")).await;
        let _lock = self.object_locks.async_lock(object_id).await;
        reord::point().await;

        // Object ID uniqueness is enforced by the `snapshot_creations` unique index
        let type_id = *T::type_ulid();
        let snapshot_version = T::snapshot_version();
        let object_json = sqlx::types::Json(&object);
        let (users_who_can_read, users_who_can_read_depends_on, _locks) = self
            .get_users_who_can_read(object_id, &*object, cb)
            .await
            .wrap_with_context(|| format!("listing users who can read object {object_id:?}"))?;
        let rdeps = self
            .get_rdeps(&mut *transaction, object_id)
            .await
            .wrap_with_context(|| format!("listing reverse dependencies of {object_id:?}"))?;
        let required_binaries = object.required_binaries();
        reord::maybe_lock().await;
        let affected = // PostgreSQL needs a lock on the unique index from here until transaction completion, hence the above reord::Lock
            sqlx::query("INSERT INTO snapshots VALUES ($1, $2, $3, TRUE, TRUE, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT DO NOTHING")
                .bind(created_at)
                .bind(type_id)
                .bind(object_id)
                .bind(normalizer_version())
                .bind(snapshot_version)
                .bind(object_json)
                .bind(users_who_can_read.iter().copied().collect::<Vec<_>>())
                .bind(&users_who_can_read_depends_on)
                .bind(&rdeps)
                .bind(&required_binaries)
                .bind(updatedness)
                .execute(&mut *transaction)
                .await
                .wrap_with_context(|| format!("inserting snapshot {created_at:?}"))?
                .rows_affected();
        reord::point().await;
        if affected != 1 {
            // Check for equality with pre-existing
            reord::point().await;
            let rdeps_to_update = sqlx::query!(
                "
                    SELECT array_length(reverse_dependents_to_update, 1) AS num_rdeps
                    FROM snapshots
                    WHERE snapshot_id = $1
                    AND type_id = $2
                    AND object_id = $3
                    AND is_creation
                    AND snapshot_version = $4
                    AND snapshot = $5
                ",
                created_at as EventId,
                *T::type_ulid() as TypeId,
                object_id as ObjectId,
                snapshot_version,
                object_json as _,
            )
            .fetch_optional(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("checking pre-existing snapshot for {created_at:?} is the same")
            })?;
            let Some(rdeps_to_update) = rdeps_to_update else {
                // There is a conflict. Is it an object conflict or an event conflict?
                reord::point().await;
                let object_exists_affected =
                    sqlx::query("SELECT 1 FROM snapshots WHERE object_id = $1")
                        .bind(object_id)
                        .execute(&mut *transaction)
                        .await
                        .wrap_with_context(|| {
                            format!("checking whether {object_id:?} already exists")
                        })?
                        .rows_affected();
                return if object_exists_affected >= 1 {
                    Err(crate::Error::ObjectAlreadyExists(object_id))
                } else {
                    Err(crate::Error::EventAlreadyExists(created_at))
                };
            };

            return Ok(Either::Right(
                rdeps_to_update.num_rdeps.is_some().then_some(created_at),
            ));
        }

        // We just inserted. Check that no event existed at this id
        reord::point().await;
        let affected = sqlx::query("SELECT event_id FROM events WHERE event_id = $1")
            .bind(created_at)
            .execute(&mut *transaction)
            .await
            .wrap_context("checking that no event existed with this id yet")?
            .rows_affected();
        if affected != 0 {
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        // Check that all required binaries are present, always as the last lock obtained in the transaction
        check_required_binaries(&mut transaction, required_binaries)
            .await
            .wrap_with_context(|| {
                format!("checking that all binaries for object {object_id:?} are already present")
            })?;

        reord::point().await;
        transaction
            .commit()
            .await
            .wrap_with_context(|| format!("committing transaction that created {object_id:?}"))?;
        reord::point().await;

        Ok(Either::Left(object))
    }

    async fn submit_impl<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
        cb: &C,
    ) -> crate::Result<Either<Arc<T>, Option<EventId>>> {
        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Acquire the locks required to submit the event
        let _lock = reord::Lock::take_named(format!("{event_id:?}")).await;
        let _lock = self.event_locks.async_lock(event_id).await;
        let _lock = reord::Lock::take_named(format!("{object_id:?}")).await;
        let _lock = self.object_locks.async_lock(object_id).await;
        reord::point().await;

        // Check the object does exist, is of the right type and is not too new
        reord::point().await;
        let creation_snapshot = sqlx::query!(
            "SELECT snapshot_id, type_id FROM snapshots WHERE object_id = $1 AND is_creation",
            object_id as ObjectId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("locking object {object_id:?} in database"))?;
        reord::point().await;
        match creation_snapshot {
            None => {
                return Err(crate::Error::ObjectDoesNotExist(object_id));
            }
            Some(s) if TypeId::from_uuid(s.type_id) != *T::type_ulid() => {
                return Err(crate::Error::WrongType {
                    object_id,
                    expected_type_id: *T::type_ulid(),
                    real_type_id: TypeId::from_uuid(s.type_id),
                })
            }
            Some(s) if s.snapshot_id >= event_id.to_uuid() => {
                return Err(crate::Error::EventTooEarly {
                    event_id,
                    object_id,
                    created_at: EventId::from_uuid(s.snapshot_id),
                });
            }
            _ => (),
        }

        // Insert the event itself
        let event_json = sqlx::types::Json(&event);
        let required_binaries = event.required_binaries();
        reord::maybe_lock().await;
        let affected =
            sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
                .bind(event_id)
                .bind(object_id)
                .bind(event_json)
                .bind(required_binaries)
                .bind(updatedness)
                .execute(&mut *transaction)
                .await
                .wrap_with_context(|| format!("inserting event {event_id:?} in database"))?
                .rows_affected();
        reord::point().await;
        if affected != 1 {
            // Check for equality with pre-existing
            reord::point().await;
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
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("checking pre-existing snapshot for {event_id:?} is the same")
            })?
            .rows_affected();
            if affected != 1 {
                return Err(crate::Error::EventAlreadyExists(event_id));
            }
            // Nothing else to do, event was already inserted. Has the current latest snapshot finished
            // updating all its rdeps?
            let last_snapshot_id = sqlx::query!(
                "
                    SELECT snapshot_id
                    FROM snapshots
                    WHERE object_id = $1
                    AND is_latest
                    AND array_length(reverse_dependents_to_update, 1) IS NOT NULL
                ",
                object_id as ObjectId,
            )
            .map(|r| EventId::from_uuid(r.snapshot_id))
            .fetch_optional(&mut *transaction)
            .await
            .wrap_context(
                "checking whether the event's reverse-dependency changes have been handled",
            )?;
            return Ok(Either::Right(last_snapshot_id));
        }

        // Clear all snapshots after the event
        reord::maybe_lock().await;
        sqlx::query("DELETE FROM snapshots WHERE object_id = $1 AND snapshot_id > $2")
            .bind(object_id)
            .bind(event_id)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("clearing all snapshots for object {object_id:?} after event {event_id:?}")
            })?;
        reord::point().await;

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
        .wrap_with_context(|| format!("fetching the last snapshot for object {object_id:?}"))?;
        let mut object =
            parse_snapshot::<T>(last_snapshot.snapshot_version, last_snapshot.snapshot)
                .wrap_with_context(|| format!("parsing last snapshot for object {object_id:?}"))?;

        // Remove the "latest snapshot" flag for the object
        // Note that this can be a no-op if the latest snapshot was already deleted above
        reord::maybe_lock().await;
        sqlx::query("UPDATE snapshots SET is_latest = FALSE WHERE object_id = $1 AND is_latest")
            .bind(object_id)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("removing latest-snapshot flag for object {object_id:?}")
            })?;
        reord::point().await;

        // Apply all events between the last snapshot (excluded) and the current event (excluded)
        if !last_snapshot.is_latest {
            let from = EventId::from_uuid(last_snapshot.snapshot_id);
            let to = EventId::from_u128(event_id.as_u128() - 1);
            apply_events_between(&mut transaction, &mut object, object_id, from, to)
                .await
                .wrap_with_context(|| {
                    format!("applying all events on {object_id:?} between {from:?} and {to:?}")
                })?;
        }

        // Add the current event to the last snapshot
        object.apply(DbPtr::from(object_id), &event);

        // Save the new snapshot (the new event was already saved above)
        let rdeps = self
            .get_rdeps(&mut *transaction, object_id)
            .await
            .wrap_with_context(|| format!("fetching reverse dependencies of {object_id:?}"))?;
        let mut _dep_locks = self
            .write_snapshot(
                &mut transaction,
                event_id,
                object_id,
                false,
                last_snapshot.is_latest,
                if last_snapshot.is_latest {
                    Some(&rdeps)
                } else {
                    None
                },
                &object,
                updatedness,
                cb,
            )
            .await
            .wrap_with_context(|| format!("writing snapshot {event_id:?} for {object_id:?}"))?;

        // If needed, re-compute the last snapshot
        if !last_snapshot.is_latest {
            // Free the locks taken above, as we don't actually need them
            std::mem::drop(_dep_locks);

            // List all the events since the inserted event
            reord::point().await;
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
                let e = e.wrap_with_context(|| {
                    format!("fetching all events for {object_id:?} after {event_id:?}")
                })?;
                last_event_id = Some(e.event_id);
                let e = serde_json::from_value::<T::Event>(e.data).wrap_with_context(|| {
                    format!(
                        "parsing event {:?} of type {:?}",
                        e.event_id,
                        T::type_ulid()
                    )
                })?;

                object.apply(DbPtr::from(object_id), &e);
            }
            std::mem::drop(events_since_inserted);

            // Save the latest snapshot
            let snapshot_id = EventId::from_uuid(
                last_event_id
                    .expect("Entered the 'recomputing last snapshot' stage without any new events"),
            );
            _dep_locks = self
                .write_snapshot(
                    &mut transaction,
                    snapshot_id,
                    object_id,
                    false,
                    true,
                    Some(&rdeps),
                    &object,
                    updatedness,
                    cb,
                )
                .await
                .wrap_with_context(|| {
                    format!("writing snapshot {snapshot_id:?} for {object_id:?}")
                })?;
        }

        // Check that all required binaries are present, always as the last lock obtained in the transaction
        check_required_binaries(&mut transaction, event.required_binaries())
            .await
            .wrap_with_context(|| {
                format!("checking that all binaries for object {object_id:?} are already present")
            })?;

        reord::point().await;
        transaction.commit().await.wrap_with_context(|| {
            format!("committing transaction adding event {event_id:?} to object {object_id:?}")
        })?;
        reord::point().await;

        Ok(Either::Left(Arc::new(object)))
    }

    pub async fn query(
        &self,
        user: User,
        type_id: TypeId,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> crate::Result<Vec<ObjectId>> {
        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Atomically perform all the reads here
        reord::point().await;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .wrap_context("setting transaction as repeatable read")?;

        let query_sql = format!(
            "
                SELECT object_id
                FROM snapshots
                WHERE is_latest
                AND type_id = $1
                AND $2 = ANY (users_who_can_read)
                AND last_modified >= $3
                AND ({})
            ",
            where_clause(&query, 4)
        );
        let min_last_modified = only_updated_since
            .map(|t| EventId::from_u128(t.as_u128().saturating_add(1))) // Handle None, Some(0) and Some(N)
            .unwrap_or(EventId::from_u128(0));
        reord::point().await;
        let mut query_sql = sqlx::query(&query_sql)
            .persistent(false) // TODO(blocked): remove when https://github.com/launchbadge/sqlx/issues/2981 is fixed
            .bind(type_id)
            .bind(user)
            .bind(min_last_modified);
        for b in binds(&query)? {
            match b {
                Bind::Json(v) => query_sql = query_sql.bind(v),
                Bind::Str(v) => query_sql = query_sql.bind(v),
                Bind::String(v) => query_sql = query_sql.bind(v),
                Bind::Decimal(v) => query_sql = query_sql.bind(v),
                Bind::I32(v) => query_sql = query_sql.bind(v),
            }
        }
        reord::point().await;
        let res = query_sql
            .map(|row| ObjectId::from_uuid(row.get(0)))
            .fetch_all(&mut *transaction)
            .await
            .wrap_with_context(|| format!("listing objects matching query {query:?}"))?;

        Ok(res)
    }

    #[cfg(test)]
    async fn assert_invariants_generic(&self) {
        // All binaries are present
        assert_eq!(
            0,
            sqlx::query(
                "
                    (
                        SELECT unnest(required_binaries)
                        FROM snapshots
                        UNION
                        SELECT unnest(required_binaries)
                        FROM events
                    )
                    EXCEPT
                    SELECT binary_id
                    FROM binaries
                ",
            )
            .execute(&self.db)
            .await
            .unwrap()
            .rows_affected()
        );

        // No event references an object without a creation snapshot
        assert_eq!(
            0,
            sqlx::query(
                "
                    SELECT object_id FROM events
                    EXCEPT
                    SELECT object_id FROM snapshots WHERE is_creation
                "
            )
            .execute(&self.db)
            .await
            .unwrap()
            .rows_affected()
        );

        // All non-creation snapshots match an event
        assert_eq!(
            0,
            sqlx::query(
                "
                    SELECT snapshot_id AS id FROM snapshots WHERE NOT is_creation
                    EXCEPT
                    SELECT event_id AS id FROM events
                "
            )
            .execute(&self.db)
            .await
            .unwrap()
            .rows_affected()
        );

        // Snapshot and events at the same time are on the same object
        assert_eq!(
            0,
            sqlx::query(
                "
                    SELECT snapshot_id FROM snapshots
                    LEFT JOIN events ON snapshots.snapshot_id = events.event_id
                    WHERE snapshots.object_id != events.object_id
                "
            )
            .execute(&self.db)
            .await
            .unwrap()
            .rows_affected()
        );

        // All objects have a single type
        assert_eq!(
            0,
            sqlx::query(
                "
                    SELECT object_id
                    FROM snapshots
                    GROUP BY object_id
                    HAVING COUNT(DISTINCT type_id) > 1
                "
            )
            .execute(&self.db)
            .await
            .unwrap()
            .rows_affected()
        )
    }

    #[cfg(test)]
    async fn assert_invariants_for<T: Object>(&self) {
        // For each object
        let objects = sqlx::query!(
            "SELECT object_id FROM snapshots WHERE type_id = $1",
            T::type_ulid() as &TypeId
        )
        .fetch_all(&self.db)
        .await
        .unwrap();
        for o in objects {
            // It has a creation and a latest snapshot
            let creation: uuid::Uuid = sqlx::query(
                "SELECT snapshot_id FROM snapshots WHERE object_id = $1 AND is_creation",
            )
            .bind(o.object_id)
            .fetch_one(&self.db)
            .await
            .unwrap()
            .get(0);
            let latest: uuid::Uuid =
                sqlx::query("SELECT snapshot_id FROM snapshots WHERE object_id = $1 AND is_latest")
                    .bind(o.object_id)
                    .fetch_one(&self.db)
                    .await
                    .unwrap()
                    .get(0);

            // They surround all events and snapshots
            assert_eq!(
                0,
                sqlx::query(
                    "
                        SELECT snapshot_id
                        FROM snapshots
                        WHERE object_id = $1
                        AND (snapshot_id < $2 OR snapshot_id > $3)
                    ",
                )
                .bind(o.object_id)
                .bind(creation)
                .bind(latest)
                .execute(&self.db)
                .await
                .unwrap()
                .rows_affected()
            );
            assert_eq!(
                0,
                sqlx::query(
                    "
                        SELECT event_id
                        FROM events
                        WHERE object_id = $1
                        AND (event_id <= $2 OR event_id > $3)
                    ",
                )
                .bind(o.object_id)
                .bind(creation)
                .bind(latest)
                .execute(&self.db)
                .await
                .unwrap()
                .rows_affected()
            );

            // Rebuilding the object gives the same snapshots
            let snapshots = sqlx::query!(
                "SELECT * FROM snapshots WHERE object_id = $1 ORDER BY snapshot_id",
                o.object_id
            )
            .fetch_all(&self.db)
            .await
            .unwrap();
            let events = sqlx::query!(
                "SELECT * FROM events WHERE object_id = $1 ORDER BY event_id",
                o.object_id
            )
            .fetch_all(&self.db)
            .await
            .unwrap();

            assert_eq!(TypeId::from_uuid(snapshots[0].type_id), *T::type_ulid());
            assert!(snapshots[0].is_creation);
            let mut object =
                parse_snapshot::<T>(snapshots[0].snapshot_version, snapshots[0].snapshot.clone())
                    .unwrap();
            assert_eq!(
                snapshots[0].required_binaries,
                object
                    .required_binaries()
                    .into_iter()
                    .map(|b| b.to_uuid())
                    .collect::<Vec<_>>()
            );

            let mut snapshot_idx = 1;
            let mut event_idx = 0;
            loop {
                if event_idx == events.len() {
                    assert_eq!(snapshot_idx, snapshots.len());
                    break;
                }
                let e = &events[event_idx];
                event_idx += 1;
                let event = serde_json::from_value::<T::Event>(e.data.clone()).unwrap();
                assert_eq!(
                    event
                        .required_binaries()
                        .into_iter()
                        .map(|b| b.to_uuid())
                        .collect::<Vec<_>>(),
                    e.required_binaries
                );
                object.apply(DbPtr::from(ObjectId::from_uuid(o.object_id)), &event);
                if snapshots[snapshot_idx].snapshot_id != e.event_id {
                    continue;
                }
                let s = &snapshots[snapshot_idx];
                snapshot_idx += 1;
                assert_eq!(TypeId::from_uuid(s.type_id), *T::type_ulid());
                let snapshot = parse_snapshot::<T>(s.snapshot_version, s.snapshot.clone()).unwrap();
                assert!(object == snapshot);
                assert_eq!(
                    s.required_binaries,
                    snapshot
                        .required_binaries()
                        .into_iter()
                        .map(|b| b.to_uuid())
                        .collect::<Vec<_>>()
                );
            }
            if events.is_empty() {
                assert!(snapshots.len() == 1);
            } else {
                assert_eq!(
                    snapshots[snapshots.len() - 1].snapshot_id,
                    events[events.len() - 1].event_id
                );
            }
            assert!(snapshots[snapshots.len() - 1].is_latest);
            assert_eq!(
                snapshots[snapshots.len() - 1]
                    .users_who_can_read
                    .as_ref()
                    .unwrap()
                    .into_iter()
                    .map(|u| User::from_uuid(*u))
                    .collect::<HashSet<_>>(),
                object.users_who_can_read(self).await.unwrap()
            );
        }
    }

    #[cfg(test)] // TODO(test-high): remove, but it's currently in use by fuzzers
    async fn change_locks(
        &self,
        _unlock: Lock,
        _then_lock: Lock,
        _object_id: ObjectId,
    ) -> crate::Result<()> {
        panic!()
    }

    pub async fn get_transaction(&self) -> crate::Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Atomically perform all the reads in this transaction
        reord::point().await;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .wrap_context("setting transaction as repeatable read")?;

        Ok(transaction)
    }

    // TODO(test-high): introduce in server-side fuzzer
    pub async fn get_all(
        &self,
        transaction: &mut sqlx::PgConnection,
        user: User,
        object_id: ObjectId,
        only_updated_since: Option<Updatedness>,
    ) -> crate::Result<ObjectData> {
        let min_last_modified = only_updated_since
            .map(|t| EventId::from_u128(t.as_u128().saturating_add(1))) // Handle None, Some(0) and Some(N)
            .unwrap_or(EventId::from_u128(0));

        // Check that our user has permissions to read the object and retrieve the type_id
        reord::point().await;
        let latest = sqlx::query!(
            "
                SELECT type_id
                FROM snapshots
                WHERE object_id = $1
                AND is_latest
                AND $2 = ANY (users_who_can_read)
            ",
            object_id as ObjectId,
            user as User,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("checking whether {user:?} can read {object_id:?}"))?;
        let Some(latest) = latest else {
            return Err(crate::Error::ObjectDoesNotExist(object_id));
        };
        let type_id = TypeId::from_uuid(latest.type_id);

        reord::point().await;
        let creation_snapshot = sqlx::query!(
            "
                SELECT snapshot_id, type_id, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                AND is_creation
                AND last_modified >= $2
            ",
            object_id as ObjectId,
            min_last_modified as EventId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("fetching creation snapshot for object {object_id:?}"))?;

        reord::point().await;
        let events = sqlx::query!(
            "
                SELECT event_id, data
                FROM events
                WHERE object_id = $1
                AND last_modified >= $2
            ",
            object_id as ObjectId,
            min_last_modified as EventId,
        )
        .map(|r| (EventId::from_uuid(r.event_id), Arc::new(r.data)))
        .fetch(&mut *transaction)
        .try_collect::<BTreeMap<EventId, Arc<serde_json::Value>>>()
        .await
        .wrap_with_context(|| format!("fetching all events for object {object_id:?}"))?;

        reord::point().await;
        let last_modified = sqlx::query!(
            "SELECT last_modified FROM snapshots WHERE object_id = $1 AND is_latest",
            object_id as ObjectId
        )
        .fetch_one(&mut *transaction)
        .await
        .wrap_context("retrieving the last_modified time")?;
        let last_modified = Updatedness::from_uuid(last_modified.last_modified);

        Ok(ObjectData {
            object_id,
            type_id,
            creation_snapshot: creation_snapshot.map(|c| {
                (
                    EventId::from_uuid(c.snapshot_id),
                    c.snapshot_version,
                    Arc::new(c.snapshot),
                )
            }),
            events,
            now_have_all_until: last_modified,
        })
    }

    pub async fn get_latest_snapshot(
        &self,
        transaction: &mut sqlx::PgConnection,
        user: User,
        object_id: ObjectId,
    ) -> crate::Result<SnapshotData> {
        reord::point().await;
        let latest_snapshot = sqlx::query!(
            "
                SELECT snapshot_id, type_id, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                AND is_latest
                AND $2 = ANY (users_who_can_read)
            ",
            object_id as ObjectId,
            user as User,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("fetching latest snapshot for object {object_id:?}"))?;
        let latest_snapshot = match latest_snapshot {
            Some(s) => s,
            None => return Err(crate::Error::ObjectDoesNotExist(object_id)),
        };
        Ok(SnapshotData {
            object_id,
            type_id: TypeId::from_uuid(latest_snapshot.type_id),
            snapshot_version: latest_snapshot.snapshot_version,
            snapshot: Arc::new(latest_snapshot.snapshot),
        })
    }
}

impl<Config: crdb_core::Config> Db for PostgresDb<Config> {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        _lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        let updatedness =
            updatedness.expect("Called PostgresDb::create without specifying updatedness");
        Ok(self
            .create_and_return_rdep_changes::<T>(object_id, created_at, object, updatedness)
            .await?
            .map(|(snap, _)| snap))
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        _force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        let updatedness =
            updatedness.expect("Called PostgresDb::create without specifying updatedness");
        Ok(self
            .submit_and_return_rdep_changes::<T>(object_id, event_id, event, updatedness)
            .await?
            .map(|(snap, _)| snap))
    }

    async fn get_latest<T: Object>(
        &self,
        _lock: Lock,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // First, check the existence and requested type
        reord::point().await;
        let latest_snapshot = sqlx::query!(
            "
                SELECT snapshot_id, type_id, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                AND is_latest
            ",
            object_id as ObjectId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("fetching latest snapshot for object {object_id:?}"))?;
        let latest_snapshot = match latest_snapshot {
            Some(s) => s,
            None => return Err(crate::Error::ObjectDoesNotExist(object_id)),
        };
        let real_type_id = TypeId::from_uuid(latest_snapshot.type_id);
        let expected_type_id = *T::type_ulid();
        if real_type_id != expected_type_id {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            });
        }

        // All good, let's parse the snapshot and return
        reord::point().await;
        let res = parse_snapshot::<T>(latest_snapshot.snapshot_version, latest_snapshot.snapshot)
            .wrap_with_context(|| format!("parsing latest snapshot for {object_id:?}"))?;
        Ok(Arc::new(res))
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        _new_created_at: EventId,
        _data: Arc<T>,
        _updatedness: Option<Updatedness>,
        _force_lock: Lock,
    ) -> crate::Result<Option<Arc<T>>> {
        panic!("Tried recreating {object_id:?} on the server, but server is supposed to only ever be the one to make recreations!")
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        panic!("Tried removing {object_id:?} from server, but server is supposed to always keep all the history!")
    }

    async fn remove_event<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
    ) -> crate::Result<()> {
        panic!("Tried removing {event_id:?} on {object_id:?} from server, but server is supposed to always keep all the history!")
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if crdb_core::hash_binary(&data) != binary_id {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        reord::maybe_lock().await;
        sqlx::query("INSERT INTO binaries VALUES ($1, $2) ON CONFLICT DO NOTHING")
            .bind(binary_id)
            .bind(&*data)
            .execute(&self.db)
            .await
            .wrap_with_context(|| format!("inserting binary {binary_id:?} into database"))?;
        reord::point().await;
        Ok(())
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        reord::point().await;
        Ok(sqlx::query!(
            "SELECT data FROM binaries WHERE binary_id = $1",
            binary_id as BinPtr
        )
        .fetch_optional(&self.db)
        .await
        .wrap_with_context(|| format!("getting {binary_id:?} from database"))?
        .map(|res| res.data.into_boxed_slice().into()))
    }

    /// Returns the number of errors that happened while re-encoding
    async fn reencode_old_versions<T: Object>(&self) -> usize {
        let mut num_errors = 0;
        let mut old_snapshots = sqlx::query(
            "
                SELECT object_id, snapshot_id
                FROM snapshots
                WHERE type_id = $1
                AND (snapshot_version < $2 OR normalizer_version < $3)
            ",
        )
        .bind(T::type_ulid())
        .bind(T::snapshot_version())
        .bind(normalizer_version())
        .fetch(&self.db);
        while let Some(s) = old_snapshots.next().await {
            let s = match s {
                Ok(s) => s,
                Err(err) => {
                    num_errors += 1;
                    tracing::error!(?err, "failed retrieving one snapshot for upgrade");
                    continue;
                }
            };
            let object_id = ObjectId::from_uuid(s.get(0));
            let snapshot_id = EventId::from_uuid(s.get(1));
            if let Err(err) = self.reencode::<T>(object_id, snapshot_id).await {
                num_errors += 1;
                tracing::error!(
                    ?err,
                    ?object_id,
                    ?snapshot_id,
                    "failed reencoding snapshot with newer version",
                );
            }
        }
        num_errors
    }
}

type TrackedLock<'cb> = (
    reord::Lock,
    <LockPool<ObjectId> as Lockable<ObjectId, ()>>::Guard<'cb>,
);

struct TrackingCanDoCallbacks<'cb, 'lockpool, C: CanDoCallbacks> {
    cb: &'cb C,
    already_taken_lock: ObjectId,
    object_locks: &'lockpool LockPool<ObjectId>,
    locks: Mutex<HashMap<ObjectId, TrackedLock<'lockpool>>>,
}

impl<'cb, 'lockpool, C: CanDoCallbacks> CanDoCallbacks
    for TrackingCanDoCallbacks<'cb, 'lockpool, C>
{
    async fn get<T: Object>(&self, object_id: crate::DbPtr<T>) -> crate::Result<Arc<T>> {
        let id = ObjectId(object_id.id);
        if id != self.already_taken_lock {
            if let hash_map::Entry::Vacant(v) = self.locks.lock().await.entry(id) {
                v.insert((
                    reord::Lock::take_named(format!("{id:?}")).await,
                    self.object_locks.async_lock(id).await,
                ));
            }
        }
        self.cb
            .get::<T>(DbPtr::from(ObjectId(object_id.id)))
            .await
            .wrap_with_context(|| format!("requesting {object_id:?} from database"))
    }
}

impl<Config: crdb_core::Config> ServerSideDb for PostgresDb<Config> {
    /// This function assumes that the lock on `object_id` is already taken.
    fn get_users_who_can_read<'a, 'ret: 'a, T: Object, C: CanDoCallbacks>(
        &'ret self,
        object_id: ObjectId,
        object: &'a T,
        cb: &'a C,
    ) -> Pin<
        Box<
            dyn 'a
                + waaaa::Future<
                    Output = anyhow::Result<(HashSet<User>, Vec<ObjectId>, Vec<ComboLock<'ret>>)>,
                >,
        >,
    > {
        Box::pin(async move {
            let cb = TrackingCanDoCallbacks::<'a, 'ret> {
                cb,
                already_taken_lock: object_id,
                object_locks: &self.object_locks,
                locks: Mutex::new(HashMap::new()),
            };

            let users_who_can_read = object.users_who_can_read(&cb).await.with_context(|| {
                format!("figuring out the list of users who can read {object_id:?}")
            })?;
            let cb_locks = cb.locks.into_inner();
            let mut users_who_can_read_depends_on = Vec::with_capacity(cb_locks.len());
            let mut locks = Vec::with_capacity(cb_locks.len());
            for (o, l) in cb_locks {
                users_who_can_read_depends_on.push(o);
                locks.push(l);
            }
            Ok((users_who_can_read, users_who_can_read_depends_on, locks))
        })
    }

    async fn recreate_at<'a, T: Object, C: CanDoCallbacks>(
        &'a self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> crate::Result<Option<(EventId, Arc<T>)>> {
        if event_id.0.timestamp_ms()
            > SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|t| t.as_millis())
                .unwrap_or(0) as u64
                - 1000 * 3600
        {
            tracing::warn!(
                "Re-creating object {object_id:?} at time {event_id:?} which is less than an hour old"
            );
        }

        reord::point().await;
        let mut transaction = self
            .db
            .begin()
            .await
            .wrap_context("acquiring postgresql transaction")?;

        // Get the creation snapshot
        reord::point().await;
        let creation_snapshot = sqlx::query!(
            "SELECT snapshot_id, type_id FROM snapshots WHERE object_id = $1 AND is_creation",
            object_id as ObjectId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| {
            format!("getting creation snapshot of {object_id:?} for re-creation")
        })?
        .ok_or(crate::Error::ObjectDoesNotExist(object_id))?;
        let real_type_id = TypeId::from_uuid(creation_snapshot.type_id);
        let expected_type_id = *T::type_ulid();
        if real_type_id != expected_type_id {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id,
                real_type_id,
            });
        }
        if EventId::from_uuid(creation_snapshot.snapshot_id) >= event_id {
            // Already created after the requested time
            return Ok(None);
        }

        // Figure out the cutoff event
        reord::point().await;
        let event = sqlx::query!(
            "
                SELECT event_id
                FROM events
                WHERE object_id = $1
                AND event_id <= $2
                ORDER BY event_id DESC
                LIMIT 1
            ",
            object_id as ObjectId,
            event_id as EventId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| {
            format!("recovering the last event for {object_id:?} before cutoff time {event_id:?}")
        })?;
        let cutoff_time = match event {
            None => return Ok(None), // Nothing to do, there was no event before the cutoff already
            Some(e) => EventId::from_uuid(e.event_id),
        };

        // Fetch the last snapshot before cutoff
        reord::point().await;
        let snapshot = sqlx::query!(
            "
                SELECT snapshot_id, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                AND snapshot_id <= $2
                ORDER BY snapshot_id DESC
                LIMIT 1
            ",
            object_id as ObjectId,
            cutoff_time as EventId,
        )
        .fetch_one(&mut *transaction)
        .await
        .wrap_with_context(|| {
            format!("fetching latest snapshot before {cutoff_time:?} for object {object_id:?}")
        })?;

        // Delete all the snapshots before cutoff
        reord::maybe_lock().await;
        sqlx::query("DELETE FROM snapshots WHERE object_id = $1 AND snapshot_id < $2")
            .bind(object_id)
            .bind(cutoff_time)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("deleting all snapshots for {object_id:?} before {cutoff_time:?}")
            })?;
        reord::point().await;

        let latest_object = if EventId::from_uuid(snapshot.snapshot_id) != cutoff_time {
            // Insert a new snapshot dated at `cutoff_time`

            // Apply all the events between latest snapshot (excluded) and asked recreation time (included)
            let mut object = parse_snapshot::<T>(snapshot.snapshot_version, snapshot.snapshot)
                .wrap_with_context(|| {
                    format!(
                        "parsing snapshot {:?} as {:?}",
                        snapshot.snapshot_id,
                        T::type_ulid()
                    )
                })?;

            let snapshot_id = EventId::from_uuid(snapshot.snapshot_id);
            apply_events_between(
                &mut transaction,
                &mut object,
                object_id,
                snapshot_id,
                cutoff_time,
            )
            .await
            .wrap_with_context(|| {
                format!(
                    "applying on {object_id:?} events between {snapshot_id:?} and {cutoff_time:?}"
                )
            })?;

            // Insert the new creation snapshot. This cannot conflict because we deleted
            // the previous creation snapshot just above. There was no snapshot at this event
            // before, so it cannot be the latest snapshot.
            // Note that we do not save the locks here. This is okay, because this is never a latest snapshot,
            // and thus cannot need the remote locks.
            self.write_snapshot(
                &mut transaction,
                cutoff_time,
                object_id,
                true,
                false,
                None, // is_latest = false, we don't care about rdeps
                &object,
                updatedness,
                cb,
            )
            .await
            .wrap_with_context(|| format!("writing snapshot {cutoff_time:?} for {object_id:?}"))?;
            object
        } else {
            // Just update the `cutoff_time` snapshot to record it's the creation snapshot
            reord::maybe_lock().await;
            let latest = sqlx::query!(
                "
                    UPDATE snapshots
                    SET is_creation = TRUE
                    WHERE snapshot_id = $1
                    RETURNING snapshot_version, snapshot
                ",
                cutoff_time as EventId,
            )
                .fetch_one(&mut *transaction)
                .await
                .wrap_with_context(|| {
                    format!(
                        "marking snapshot {cutoff_time:?} as the creation one for {object_id:?} and retrieving its data"
                    )
                })?;
            let object = parse_snapshot::<T>(latest.snapshot_version, latest.snapshot)
                .wrap_context("deserializing snapshot data")?;
            reord::point().await;
            object
        };

        // We now have all the new information. We can delete the events.
        reord::maybe_lock().await;
        sqlx::query("DELETE FROM events WHERE object_id = $1 AND event_id <= $2")
            .bind(object_id)
            .bind(cutoff_time)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("deleting all events for {object_id:?} before {cutoff_time:?}")
            })?;
        reord::point().await;

        // Mark the corresponding latest snapshot as updated
        reord::maybe_lock().await;
        let affected = sqlx::query(
            "UPDATE snapshots SET last_modified = $1 WHERE is_latest AND object_id = $2",
        )
        .bind(updatedness)
        .bind(object_id)
        .execute(&mut *transaction)
        .await
        .wrap_with_context(|| format!("failed marking last snapshot of {object_id:?} as modified"))?
        .rows_affected();
        reord::point().await;
        assert!(
            affected == 1,
            "Object {object_id:?} did not have a latest snapshot, something went very wrong"
        );

        // Finally, commit the transaction
        reord::maybe_lock().await;
        transaction.commit().await.wrap_with_context(|| {
            format!("committing transaction that recreated {object_id:?} at {cutoff_time:?}")
        })?;
        reord::point().await;

        Ok(Some((cutoff_time, Arc::new(latest_object))))
    }

    async fn create_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Updatedness,
    ) -> crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>> {
        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::create after CacheDb went away");

        let res = self
            .create_impl(object_id, created_at, object, updatedness, &*cache_db)
            .await?;
        match res {
            Either::Left(res) => {
                // Newly inserted, update rdeps and return
                // Update the reverse-dependencies, now that we have updated the object itself.
                let rdeps = self
                    .update_rdeps(object_id, &*cache_db)
                    .await
                    .wrap_with_context(|| {
                        format!("updating permissions for reverse-dependencies of {object_id:?}")
                    })?;

                Ok(Some((res, rdeps)))
            }
            Either::Right(None) => {
                // Was already present, and has no pending rdeps
                Ok(None)
            }
            Either::Right(Some(_snapshot_id)) => {
                // Was already present, but had a task ongoing to update the rdeps
                // TODO(perf-high): this will duplicate the work done by the other create call
                self.update_rdeps(object_id, &*cache_db)
                    .await
                    .wrap_with_context(|| {
                        format!("updating permissions for reverse-dependencies of {object_id:?}")
                    })?;
                Ok(None)
            }
        }
    }

    async fn submit_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
    ) -> crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>> {
        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::submit after CacheDb went away");

        let res = self
            .submit_impl(object_id, event_id, event, updatedness, &*cache_db)
            .await?;
        match res {
            Either::Left(res) => {
                // Newly inserted, update rdeps and return
                // Update the reverse-dependencies, now that we have updated the object itself.
                let rdeps = self
                    .update_rdeps(object_id, &*cache_db)
                    .await
                    .wrap_with_context(|| {
                        format!("updating permissions for reverse-dependencies of {object_id:?}")
                    })?;

                Ok(Some((res, rdeps)))
            }
            Either::Right(None) => {
                // Was already present, and has no pending rdeps
                Ok(None)
            }
            Either::Right(Some(_snapshot_id)) => {
                // Was already present, but had a task ongoing to update the rdeps
                // TODO(perf-high): this will duplicate the work done by the other create call
                self.update_rdeps(object_id, &*cache_db)
                    .await
                    .wrap_with_context(|| {
                        format!("updating permissions for reverse-dependencies of {object_id:?}")
                    })?;
                Ok(None)
            }
        }
    }
}

async fn check_required_binaries(
    t: &mut sqlx::PgConnection,
    mut binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    // FOR KEY SHARE: prevent DELETE of the binaries while `t` is running
    reord::maybe_lock().await;
    let present_ids =
        sqlx::query("SELECT binary_id FROM binaries WHERE binary_id = ANY ($1) FOR KEY SHARE")
            .bind(&binaries)
            .fetch_all(&mut *t)
            .await
            .wrap_context("listing binaries already present in database")?;
    reord::point().await;
    binaries.retain(|b| {
        present_ids
            .iter()
            .all(|i| i.get::<uuid::Uuid, _>(0) != b.to_uuid())
    });
    if !binaries.is_empty() {
        return Err(crate::Error::MissingBinaries(binaries));
    }
    Ok(())
}

/// `from` is excluded
/// `to` is included
async fn apply_events_between<T: Object>(
    transaction: &mut sqlx::PgConnection,
    object: &mut T,
    object_id: ObjectId,
    from: EventId,
    to: EventId,
) -> anyhow::Result<()> {
    reord::point().await;
    let mut events = sqlx::query!(
        "
            SELECT event_id, data
            FROM events
            WHERE object_id = $1
            AND event_id > $2
            AND event_id <= $3
            ORDER BY event_id ASC
        ",
        object_id as ObjectId,
        from as EventId,
        to as EventId,
    )
    .fetch(&mut *transaction);
    while let Some(e) = events.next().await {
        let e = e.with_context(|| {
            format!("fetching all events for {object_id:?} betwen {from:?} and {to:?}")
        })?;
        let e = serde_json::from_value::<T::Event>(e.data).with_context(|| {
            format!(
                "parsing event {:?} of type {:?}",
                e.event_id,
                T::type_ulid()
            )
        })?;

        object.apply(DbPtr::from(object_id), &e);
    }
    Ok(())
}

pub fn where_clause(this: &Query, first_idx: usize) -> String {
    let mut res = String::new();
    let mut bind_idx = first_idx;
    add_to_where_clause(&mut res, &mut bind_idx, this);
    res
}

pub fn binds(this: &Query) -> crate::Result<Vec<Bind<'_>>> {
    let mut res = Vec::new();
    add_to_binds(&mut res, this)?;
    Ok(res)
}

fn add_to_where_clause(res: &mut String, bind_idx: &mut usize, query: &Query) {
    let mut initial_bind_idx = *bind_idx;
    match query {
        Query::All(v) => {
            res.push_str("TRUE");
            for q in v {
                res.push_str(" AND (");
                add_to_where_clause(&mut *res, &mut *bind_idx, q);
                res.push(')');
            }
        }
        Query::Any(v) => {
            res.push_str("FALSE");
            for q in v {
                res.push_str(" OR (");
                add_to_where_clause(&mut *res, &mut *bind_idx, q);
                res.push(')');
            }
        }
        Query::Not(q) => {
            res.push_str("NOT (");
            add_to_where_clause(&mut *res, &mut *bind_idx, q);
            res.push(')');
        }
        Query::Eq(path, _) => {
            res.push_str("COALESCE(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(&format!(" = ${}, FALSE)", bind_idx));
            *bind_idx += 1;
        }
        Query::Le(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric <= ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Lt(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric < ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Ge(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric >= ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Gt(path, _) => {
            res.push_str("CASE WHEN jsonb_typeof(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(") = 'number' THEN (");
            add_path_to_clause(&mut *res, &mut initial_bind_idx, path);
            res.push_str(&format!(")::numeric > ${} ELSE FALSE END", bind_idx));
            *bind_idx += 1;
        }
        Query::Contains(path, _) => {
            res.push_str("COALESCE(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            res.push_str(&format!(" @> ${}, FALSE)", bind_idx));
            *bind_idx += 1;
        }
        Query::ContainsStr(path, pat) => {
            res.push_str("COALESCE(to_tsvector(");
            add_path_to_clause(&mut *res, &mut *bind_idx, path);
            // If the pattern is only spaces, then postgresql wrongly returns `false`. But we do want
            // to check that the field does exist. So add an IS NOT NULL in that case
            let or_empty_pat = match pat.chars().all(|c| c == ' ') {
                true => "IS NOT NULL",
                false => "",
            };
            res.push_str(&format!(
                "->'_crdb-normalized') @@ phraseto_tsquery(${}) {or_empty_pat}, FALSE)",
                bind_idx,
            ));
            *bind_idx += 1;
        }
    }
}

fn add_path_to_clause(res: &mut String, bind_idx: &mut usize, path: &[JsonPathItem]) {
    if let Some(JsonPathItem::Id(i)) = path.last() {
        if *i == -1 || *i == 0 {
            // PostgreSQL currently treats numerics as arrays of size 1
            // See also https://www.postgresql.org/message-id/87h6jbbxma.fsf%40coegni.ekleog.org
            res.push_str("CASE WHEN jsonb_typeof(snapshot");
            for (i, _) in path[..path.len() - 1].iter().enumerate() {
                res.push_str(&format!("->${}", *bind_idx + i));
            }
            res.push_str(") = 'array' THEN ")
        }
    }
    res.push_str("snapshot");
    for _ in path {
        res.push_str(&format!("->${bind_idx}"));
        *bind_idx += 1;
    }
    if let Some(JsonPathItem::Id(i)) = path.last() {
        if *i == -1 || *i == 0 {
            res.push_str(" ELSE NULL END");
        }
    }
}

fn add_path_to_binds<'a>(res: &mut Vec<Bind<'a>>, path: &'a [JsonPathItem]) {
    for p in path {
        match p {
            JsonPathItem::Key(k) => res.push(Bind::Str(k)),
            JsonPathItem::Id(i) => res.push(Bind::I32(*i)),
        }
    }
}

pub enum Bind<'a> {
    Json(&'a serde_json::Value),
    Str(&'a str),
    String(String),
    Decimal(Decimal),
    I32(i32),
}

fn add_to_binds<'a>(res: &mut Vec<Bind<'a>>, query: &'a Query) -> crate::Result<()> {
    match query {
        Query::All(v) => {
            for q in v {
                add_to_binds(&mut *res, q)?;
            }
        }
        Query::Any(v) => {
            for q in v {
                add_to_binds(&mut *res, q)?;
            }
        }
        Query::Not(q) => {
            add_to_binds(&mut *res, q)?;
        }
        Query::Eq(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Json(v));
        }
        Query::Le(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(*v));
        }
        Query::Lt(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(*v));
        }
        Query::Ge(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(*v));
        }
        Query::Gt(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Decimal(*v));
        }
        Query::Contains(p, v) => {
            add_path_to_binds(&mut *res, p);
            res.push(Bind::Json(v));
        }
        Query::ContainsStr(p, v) => {
            add_path_to_binds(&mut *res, p);
            crdb_core::check_string(v)?;
            res.push(Bind::String(crdb_core::normalize(v)));
        }
    }
    Ok(())
}
