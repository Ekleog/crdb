use crate::{
    api::ApiConfig, cache::CacheDb, messages::ServerMessage, EventId, ObjectId, SessionToken,
    Timestamp, Updatedness,
};
use anyhow::Context;
use multimap::MultiMap;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

pub struct Server<C: ServerConfig> {
    _cache_db: Arc<CacheDb<PostgresDb<C>>>, // TODO(api): use this to implement the server
    postgres_db: Arc<PostgresDb<C>>,
    _cleanup_token: tokio_util::sync::DropGuard,
    // TODO(api): use all the below
    _watchers: HashMap<ObjectId, HashSet<SessionToken>>,
    _sessions: MultiMap<SessionToken, mpsc::UnboundedSender<ServerMessage>>,
}

impl<C: ServerConfig> Server<C> {
    /// Returns both the server itself, as well as a `JoinHandle` that will resolve once all the operations
    /// needed for database upgrading are over. The handle resolves with the number of errors that occurred
    /// during the upgrade, normal runs would return 0. There will be one error message in the tracing logs
    /// for each such error.
    pub async fn new<Tz>(
        config: C,
        db: sqlx::PgPool,
        cache_watermark: usize,
        vacuum_schedule: ServerVacuumSchedule<Tz>,
    ) -> anyhow::Result<(Self, JoinHandle<usize>)>
    where
        Tz: 'static + Send + chrono::TimeZone,
        Tz::Offset: Send,
    {
        let _ = config; // ignore argument

        // Check that all type ULIDs are distinct
        <C::ApiConfig as ApiConfig>::check_ulids();

        // Connect to the database and setup the cache
        let (postgres_db, cache_db) = postgres_db::PostgresDb::connect(db, cache_watermark).await?;

        // Start the upgrading task
        let upgrade_handle = tokio::task::spawn(C::reencode_old_versions(postgres_db.clone()));

        // Setup the auto-vacuum task
        let cancellation_token = CancellationToken::new();
        tokio::task::spawn({
            let postgres_db = postgres_db.clone();
            let cancellation_token = cancellation_token.clone();
            async move {
                for next_time in vacuum_schedule.schedule.upcoming(vacuum_schedule.timezone) {
                    let sleep_for = next_time.signed_duration_since(chrono::Utc::now());
                    let sleep_for = sleep_for
                        .to_std()
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    tokio::select! {
                        _ = tokio::time::sleep(sleep_for) => (),
                        _ = cancellation_token.cancelled() => break,
                    }
                    let no_new_changes_before = vacuum_schedule.recreate_older_than.map(|d| {
                        EventId(Ulid::from_parts(
                            Timestamp::from(SystemTime::now() - d).time_ms(),
                            u128::MAX,
                        ))
                    });
                    let kill_sessions_older_than = vacuum_schedule
                        .kill_sessions_older_than
                        .map(|d| Timestamp::from(SystemTime::now() - d));
                    if let Err(err) = postgres_db
                        .vacuum(
                            no_new_changes_before,
                            Updatedness::now(), // TODO(api): should think some more about how the server generates and pushes its `Updatedness`.
                            kill_sessions_older_than,
                            |_| unimplemented!(), // TODO(api)
                        )
                        .await
                    {
                        tracing::error!(?err, "scheduled vacuum failed");
                    }
                }
            }
        });

        // Finally, return the information
        let this = Server {
            _cache_db: cache_db,
            postgres_db,
            _cleanup_token: cancellation_token.drop_guard(),
            _watchers: HashMap::new(),
            _sessions: MultiMap::new(),
        };
        Ok((this, upgrade_handle))
    }

    // TODO(api): replace with a function that just takes in a websocket io stream
    pub async fn serve(&self, addr: &SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding to {addr:?}"))?;
        axum::serve(listener, axum::Router::new())
            .await
            .context("serving axum webserver")
    }

    /// Cleans up and optimizes up the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    pub async fn vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        kill_sessions_older_than: Option<Timestamp>,
    ) -> crate::Result<()> {
        self.postgres_db
            .vacuum(
                no_new_changes_before,
                Updatedness::now(), // TODO(api): should think some more about how the server generates and pushes its `Updatedness`.
                kill_sessions_older_than,
                |_| unimplemented!(), // TODO(api)
            )
            .await
    }
}

pub struct ServerVacuumSchedule<Tz: chrono::TimeZone> {
    schedule: cron::Schedule,
    timezone: Tz,
    recreate_older_than: Option<Duration>,
    kill_sessions_older_than: Option<Duration>,
}

impl<Tz: chrono::TimeZone> ServerVacuumSchedule<Tz> {
    pub fn new(schedule: cron::Schedule, timezone: Tz) -> ServerVacuumSchedule<Tz> {
        ServerVacuumSchedule {
            schedule,
            timezone,
            recreate_older_than: None,
            kill_sessions_older_than: None,
        }
    }

    pub fn recreate_older_than(mut self, age: Duration) -> Self {
        self.recreate_older_than = Some(age);
        self
    }

    pub fn kill_sessions_older_than(mut self, age: Duration) -> Self {
        self.kill_sessions_older_than = Some(age);
        self
    }
}
