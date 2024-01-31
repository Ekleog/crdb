use super::ServerConfig;
use crate::{
    cache::CacheDb,
    db_trait::Db,
    error::ResultExt,
    fts,
    messages::{ObjectData, Update, UpdateData},
    object::parse_snapshot,
    query::Bind,
    BinPtr, CanDoCallbacks, DbPtr, Event, EventId, Object, ObjectId, Query, Session, SessionRef,
    SessionToken, Timestamp, TypeId, Updatedness, User,
};
use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use lockable::{LockPool, Lockable};
use sqlx::Row;
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    marker::PhantomData,
    sync::{Arc, Weak},
    time::SystemTime,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests;

pub struct PostgresDb<Config: ServerConfig> {
    db: sqlx::PgPool,
    cache_db: Weak<CacheDb<PostgresDb<Config>>>,
    event_locks: LockPool<EventId>,
    // TODO(low): make into a RwLockPool, which locks the snapshot fields
    // create and submit take a write(), get and update_users_who_can_read
    // take a read(). This should significantly improve performance
    object_locks: LockPool<ObjectId>,
    _phantom: PhantomData<Config>,
}

pub type ComboLock<'a> = (
    reord::Lock,
    <lockable::LockPool<ObjectId> as lockable::Lockable<ObjectId, ()>>::Guard<'a>,
);

impl<Config: ServerConfig> PostgresDb<Config> {
    pub async fn connect(
        db: sqlx::PgPool,
        cache_watermark: usize,
    ) -> anyhow::Result<(Arc<PostgresDb<Config>>, Arc<CacheDb<PostgresDb<Config>>>)> {
        sqlx::migrate!("src/server/migrations")
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
            .bind(session.login_time.time_ms_i()?)
            .bind(session.last_active.time_ms_i()?)
            .bind(session.expiration_time.map(|t| t.time_ms_i()).transpose()?)
            .execute(&self.db)
            .await
            .wrap_with_context(|| {
                format!("logging in new session {token:?} with data {session:?}")
            })?;
        Ok((token, session.session_ref))
    }

    pub async fn resume_session(&self, token: SessionToken) -> crate::Result<Session> {
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
            login_time: Timestamp::from_i64_ms(res.login_time),
            last_active: Timestamp::from_i64_ms(res.last_active),
            expiration_time: res.expiration_time.map(Timestamp::from_i64_ms),
        })
    }

    pub async fn mark_session_active(
        &self,
        token: SessionToken,
        at: Timestamp,
    ) -> crate::Result<()> {
        let affected = sqlx::query("UPDATE sessions SET last_active = $1 WHERE session_token = $2")
            .bind(at.time_ms_i()?)
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
                login_time: Timestamp::from_i64_ms(r.login_time),
                last_active: Timestamp::from_i64_ms(r.last_active),
                expiration_time: r.expiration_time.map(Timestamp::from_i64_ms),
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
        if s.normalizer_version >= fts::normalizer_version()
            && s.snapshot_version >= T::snapshot_version()
        {
            return Ok(());
        }

        // If not, we still need to parse-and-reencode it
        let snapshot = parse_snapshot::<T>(s.snapshot_version, s.snapshot)
            .wrap_with_context(|| format!("parsing snapshot {snapshot_id:?}"))?;
        sqlx::query("UPDATE snapshots SET snapshot = $1 WHERE snapshot_id = $2")
            .bind(sqlx::types::Json(&snapshot))
            .bind(snapshot_id)
            .execute(&self.db)
            .await
            .wrap_with_context(|| format!("re-encoding snapshot data for {snapshot_id:?}"))?;

        Ok(())
    }

    pub async fn reencode_old_versions<T: Object>(&self) -> usize {
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
        .bind(fts::normalizer_version())
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

    /// Cleans up and optimizes up the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    pub async fn vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<Timestamp>,
        mut notify_recreation: impl FnMut(Update, Vec<User>),
    ) -> crate::Result<()> {
        // TODO(low): do not vacuum away binaries that have been uploaded less than an hour ago
        // TODO(low): also keep an "upload time" field on events, and allow fetching just the new events for an object
        if let Some(t) = kill_sessions_older_than {
            // Discard all sessions that were last active too long ago
            reord::point().await;
            sqlx::query!(
                "DELETE FROM sessions WHERE last_active < $1",
                t.time_ms() as i64
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
                        &self,
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
                                type_id,
                                object_id,
                                data: UpdateData::Creation {
                                    created_at: new_created_at,
                                    snapshot_version,
                                    data,
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
        reord::maybe_lock().await;
        sqlx::query("VACUUM ANALYZE")
            .execute(&self.db)
            .await
            .wrap_context("vacuuming database")?;
        reord::point().await;

        Ok(())
    }

    /// This function assumes that the lock on `object_id` is already taken.
    pub async fn get_users_who_can_read<'a, T: Object, C: CanDoCallbacks>(
        &'a self,
        object_id: &ObjectId,
        object: &T,
        cb: &C,
    ) -> anyhow::Result<(Vec<User>, Vec<ObjectId>, Vec<ComboLock<'a>>)> {
        struct TrackingCanDoCallbacks<'a, 'b, C: CanDoCallbacks> {
            cb: &'a C,
            already_taken_lock: ObjectId,
            object_locks: &'b LockPool<ObjectId>,
            locks: Mutex<
                HashMap<
                    ObjectId,
                    (
                        reord::Lock,
                        <LockPool<ObjectId> as Lockable<ObjectId, ()>>::Guard<'b>,
                    ),
                >,
            >,
        }

        impl<'a, 'b, C: CanDoCallbacks> crate::object::private::Sealed
            for TrackingCanDoCallbacks<'a, 'b, C>
        {
        }

        impl<'a, 'b, C: CanDoCallbacks> CanDoCallbacks for TrackingCanDoCallbacks<'a, 'b, C> {
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

        let cb = TrackingCanDoCallbacks {
            cb,
            already_taken_lock: *object_id,
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
    ) -> anyhow::Result<()> {
        // Take the locks
        let _lock = (
            reord::Lock::take_named(format!("{object_id:?}")).await,
            self.object_locks.async_lock(object_id).await,
        );

        // Start the transaction
        let mut transaction = self
            .db
            .begin()
            .await
            .context("starting postgresql transaction")?;

        // Retrieve the snapshot
        reord::point().await;
        let res = sqlx::query!(
            "
                SELECT type_id, snapshot_version, snapshot FROM snapshots
                WHERE object_id = $1
                AND is_latest
            ",
            object_id as ObjectId,
        )
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("fetching latest snapshot for object {object_id:?}"))?;

        // Figure out the new value of users_who_can_read
        let (users_who_can_read, users_who_can_read_depends_on, _locks) =
            Config::get_users_who_can_read(
                &self,
                object_id,
                TypeId::from_uuid(res.type_id),
                res.snapshot_version,
                res.snapshot,
                cb,
            )
            .await
            .with_context(|| format!("updating users_who_can_read cache of {object_id:?}"))?;

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
        .bind(users_who_can_read)
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

        // Commit the transaction
        reord::point().await;
        transaction.commit().await.with_context(|| {
            format!("committing transaction that updated users_who_can_read of {object_id:?}")
        })?;
        reord::point().await;
        Ok(())
    }

    async fn update_rdeps<C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        cb: &C,
    ) -> anyhow::Result<()> {
        let rdeps = self.get_rdeps(&self.db, object_id).await?;
        for o in rdeps {
            if o != object_id {
                self.update_users_who_can_read(object_id, o, cb)
                    .await
                    .with_context(|| format!("updating users_who_can_read field for {o:?} on behalf of {object_id:?}"))?;
            }
        }
        Ok(())
    }

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
                .get_users_who_can_read::<T, _>(&object_id, object, cb)
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
        .bind(&fts::normalizer_version())
        .bind(T::snapshot_version())
        .bind(sqlx::types::Json(object))
        .bind(users_who_can_read)
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
    ) -> crate::Result<Option<Arc<T>>> {
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
            .get_users_who_can_read(&object_id, &*object, cb)
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
                .bind(&fts::normalizer_version())
                .bind(snapshot_version)
                .bind(object_json)
                .bind(&users_who_can_read)
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
            let affected = sqlx::query(
                "
                    SELECT 1 FROM snapshots
                    WHERE snapshot_id = $1
                    AND type_id = $2
                    AND object_id = $3
                    AND is_creation
                    AND snapshot_version = $4
                    AND snapshot = $5
                ",
            )
            .bind(created_at)
            .bind(T::type_ulid())
            .bind(object_id)
            .bind(snapshot_version)
            .bind(object_json)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| {
                format!("checking pre-existing snapshot for {created_at:?} is the same")
            })?
            .rows_affected();
            if affected != 1 {
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
            }

            return Ok(None);
        }

        // We just inserted. Check that no event existed at this id
        reord::point().await;
        let affected = sqlx::query("SELECT event_id FROM events WHERE event_id = $1")
            .bind(created_at)
            .execute(&mut *transaction)
            .await
            .wrap_with_context(|| format!("checking that no event existed with this id yet"))?
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

        Ok(Some(object))
    }

    async fn submit_impl<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
        cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
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
        let rdeps = self
            .get_rdeps(&mut *transaction, object_id)
            .await
            .wrap_with_context(|| format!("fetching reverse dependencies of {object_id:?}"))?;

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
            sqlx::query("INSERT INTO events VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING")
                .bind(event_id)
                .bind(object_id)
                .bind(event_json)
                .bind(required_binaries)
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
            .execute(&self.db)
            .await
            .wrap_with_context(|| {
                format!("checking pre-existing snapshot for {event_id:?} is the same")
            })?
            .rows_affected();
            if affected != 1 {
                return Err(crate::Error::EventAlreadyExists(event_id));
            }
            // Nothing else to do, event was already inserted
            return Ok(None);
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
            apply_events_between(&mut *transaction, &mut object, object_id, from, to)
                .await
                .wrap_with_context(|| {
                    format!("applying all events on {object_id:?} between {from:?} and {to:?}")
                })?;
        }

        // Add the current event to the last snapshot
        object.apply(DbPtr::from(object_id), &event);

        // Save the new snapshot (the new event was already saved above)
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

        Ok(Some(Arc::new(object)))
    }

    /// This function assumes that the lock on `object_id` is already taken
    ///
    /// Returns `true` iff the object actually changed
    pub async fn recreate_impl<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &C,
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
                &mut *transaction,
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
                &mut *transaction,
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
            query.where_clause(4)
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
        for b in query.binds()? {
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
                snapshots[snapshots.len() - 1].users_who_can_read,
                Some(
                    object
                        .users_who_can_read(self)
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|u| u.to_uuid())
                        .collect::<Vec<_>>()
                )
            );
        }
    }

    #[cfg(feature = "_tests")]
    #[allow(dead_code)] // Used by fuzzers
    async fn unlock(&self, _object_id: ObjectId) -> crate::Result<()> {
        panic!()
    }

    pub async fn get_all(&self, object_id: ObjectId) -> crate::Result<ObjectData> {
        // TODO(server): take as parameter if-modified-since, and only return the things that are more recent than that
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

        reord::point().await;
        let creation_snapshot = sqlx::query!(
            "
                SELECT snapshot_id, type_id, snapshot_version, snapshot
                FROM snapshots
                WHERE object_id = $1
                AND is_creation
            ",
            object_id as ObjectId,
        )
        .fetch_optional(&mut *transaction)
        .await
        .wrap_with_context(|| format!("fetching creation snapshot for object {object_id:?}"))?;
        let creation_snapshot = match creation_snapshot {
            Some(s) => s,
            None => return Err(crate::Error::ObjectDoesNotExist(object_id)),
        };

        reord::point().await;
        let events = sqlx::query!(
            "SELECT event_id, data FROM events WHERE object_id = $1 ORDER BY event_id",
            object_id as ObjectId
        )
        .map(|r| (EventId::from_uuid(r.event_id), r.data))
        .fetch(&mut *transaction)
        .try_collect::<BTreeMap<EventId, serde_json::Value>>()
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
            type_id: TypeId::from_uuid(creation_snapshot.type_id),
            creation_snapshot: Some((
                EventId::from_uuid(creation_snapshot.snapshot_id),
                creation_snapshot.snapshot_version,
                creation_snapshot.snapshot,
            )),
            events,
            now_have_all_until: last_modified,
        })
    }
}

impl<Config: ServerConfig> Db for PostgresDb<Config> {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        _lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::create after CacheDb went away");
        let updatedness =
            updatedness.expect("Called PostgresDb::create without specifying updatedness");

        let res = self
            .create_impl(object_id, created_at, object, updatedness, &*cache_db)
            .await?;

        // Update the reverse-dependencies, now that we have updated the object itself.
        self.update_rdeps(object_id, &*cache_db)
            .await
            .wrap_with_context(|| {
                format!("updating permissions for reverse-dependencies of {object_id:?}")
            })?;

        Ok(res)
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        _force_lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        let cache_db = self
            .cache_db
            .upgrade()
            .expect("Called PostgresDb::submit after CacheDb went away");
        let updatedness =
            updatedness.expect("Called PostgresDb::create without specifying updatedness");

        let res = self
            .submit_impl(object_id, event_id, event, updatedness, &*cache_db)
            .await?;

        // Update all the other objects that depend on this one
        self.update_rdeps(object_id, &*cache_db)
            .await
            .wrap_with_context(|| {
                format!("updating permissions of reverse-dependencies fo {object_id:?}")
            })?;

        Ok(res)
    }

    async fn get_latest<T: Object>(
        &self,
        _lock: bool,
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
        _force_lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        panic!("Tried recreating {object_id:?} on the server, but server is supposed to only ever be the one to make recreations!")
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        panic!("Tried removing {object_id:?} from server, but server is supposed to always keep all the history!")
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if crate::hash_binary(&data) != binary_id {
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
}

async fn check_required_binaries(
    t: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mut binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    // FOR KEY SHARE: prevent DELETE of the binaries while `t` is running
    reord::maybe_lock().await;
    let present_ids =
        sqlx::query("SELECT binary_id FROM binaries WHERE binary_id = ANY ($1) FOR KEY SHARE")
            .bind(&binaries)
            .fetch_all(&mut **t)
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
