use crate::{
    api::{ApiConfig, ServerMessage},
    cache::CacheDb,
    ObjectId, Session, SessionToken, Timestamp,
};
use anyhow::Context;
use axum::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{sync::mpsc, task::JoinHandle};

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    // TODO(api): make sure protocol is two-step, with server sending random data to client and
    // only then client answering with the Authenticator? This'd make it possible to use
    // a public key to auth
    fn authenticate(data: Auth) -> Result<Session, (StatusCode, String)>;
}

pub struct Server<C: ServerConfig> {
    db: Arc<CacheDb<PostgresDb<C>>>,
    postgres_db: Arc<PostgresDb<C>>,
    // TODO(api): use all the below
    _watchers: HashMap<ObjectId, HashSet<SessionToken>>,
    _sessions: HashMap<SessionToken, mpsc::UnboundedSender<ServerMessage>>,
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
        vacuum_schedule: VacuumSchedule<Tz>,
    ) -> anyhow::Result<(Self, JoinHandle<usize>)>
    where
        Tz: 'static + Send + chrono::TimeZone,
        Tz::Offset: Send,
    {
        let _ = config; // ignore argument

        // Check that all type ULIDs are distinct
        <C::ApiConfig as ApiConfig>::check_ulids();

        // Connect to the database and setup the cache
        let postgres_db = Arc::new(postgres_db::PostgresDb::connect(db).await?);
        let db = CacheDb::new::<C::ApiConfig>(postgres_db.clone(), cache_watermark);

        // Start the upgrading task
        let upgrade_handle = tokio::task::spawn(C::reencode_old_versions(postgres_db.clone()));

        // Setup the auto-vacuum task
        tokio::task::spawn({
            let postgres_db = postgres_db.clone();
            let db = db.clone();
            async move {
                for next_time in vacuum_schedule.schedule.upcoming(vacuum_schedule.timezone) {
                    let sleep_for = next_time.signed_duration_since(chrono::Utc::now());
                    let sleep_for = sleep_for
                        .to_std()
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    tokio::time::sleep(sleep_for).await;
                    // TODO(high): regularize all time handling to all be done in UTC
                    let no_new_changes_before = vacuum_schedule.recreate_older_than.map(|d| {
                        Timestamp::from_ms(
                            (SystemTime::now() - d)
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                        )
                    });
                    let kill_sessions_older_than =
                        vacuum_schedule.kill_sessions_older_than.map(|d| {
                            Timestamp::from_ms(
                                (SystemTime::now() - d)
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64,
                            )
                        });
                    if let Err(err) = postgres_db
                        .vacuum(
                            no_new_changes_before,
                            kill_sessions_older_than,
                            &*db,
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
            db,
            postgres_db,
            _watchers: HashMap::new(),
            _sessions: HashMap::new(),
        };
        Ok((this, upgrade_handle))
    }

    pub async fn serve(&self, addr: &SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding to {addr:?}"))?;
        self.db.reduce_size_to(1024).await; // shut dead code warning up for now
        self.db.clear_cache().await; // shut dead code warning up for now
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
        no_new_changes_before: Option<Timestamp>,
        kill_sessions_older_than: Option<Timestamp>,
    ) -> crate::Result<()> {
        self.postgres_db
            .vacuum(
                no_new_changes_before,
                kill_sessions_older_than,
                &*self.db,
                |_| unimplemented!(), // TODO(api)
            )
            .await
    }
}

pub struct VacuumSchedule<Tz: chrono::TimeZone> {
    schedule: cron::Schedule,
    timezone: Tz,
    recreate_older_than: Option<Duration>,
    kill_sessions_older_than: Option<Duration>,
}

impl<Tz: chrono::TimeZone> VacuumSchedule<Tz> {
    pub fn new(schedule: cron::Schedule, timezone: Tz) -> VacuumSchedule<Tz> {
        VacuumSchedule {
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
