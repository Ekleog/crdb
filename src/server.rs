use crate::{
    api::ApiConfig, cache::CacheDb, messages::Updates, EventId, SessionRef, Timestamp, Updatedness,
};
use anyhow::{anyhow, Context};
use multimap::MultiMap;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

pub struct Server<C: ServerConfig> {
    _cache_db: Arc<CacheDb<PostgresDb<C>>>, // TODO(api): use this to implement the server
    postgres_db: Arc<PostgresDb<C>>,
    updatedness_requester:
        mpsc::UnboundedSender<oneshot::Sender<(Updatedness, oneshot::Sender<Updates>)>>,
    _cleanup_token: tokio_util::sync::DropGuard,
    // For each ongoing session, a way to push updates. Each update is both the list of updates itself, and
    // the new latest snapshot for query matching, available if the latest snapshot actually changed
    _sessions: MultiMap<SessionRef, mpsc::UnboundedSender<(Updates, Option<serde_json::Value>)>>, // TODO(api): use
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

        // Setup the update reorderer task
        let (updatedness_requester, mut updatedness_request_receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<(Updatedness, oneshot::Sender<Updates>)>>();
        let (update_sender, mut update_receiver) = mpsc::unbounded_channel();
        tokio::task::spawn(async move {
            // Updatedness request handler
            let mut generator = ulid::Generator::new();
            // No cancellation token needed, closing the sender will naturally close this task
            while let Some(requester) = updatedness_request_receiver.recv().await {
                // TODO(low): see also https://github.com/dylanhart/ulid-rs/issues/71
                let updatedness = Updatedness(generator.generate().expect(
                    "you're either very unlucky, or generated 2**80 updates within one millisecond",
                ));
                let (sender, receiver) = oneshot::channel();
                if let Err(_) = update_sender.send(receiver) {
                    tracing::error!(
                        "Update reorderer task went away before updatedness request handler task"
                    );
                }
                let _ = requester.send((updatedness, sender)); // Ignore any failures, they'll free the slot anyway
            }
        });
        tokio::task::spawn(async move {
            // Actual update reorderer
            // No cancellation token needed, closing the senders will naturally close this task
            while let Some(update_receiver) = update_receiver.recv().await {
                // Ignore the case where the slot sender was dropped
                if let Ok(updates) = update_receiver.await {
                    let _ = updates; // TODO(api): actually fan the update out to all subscribed listeners
                }
            }
        });

        // Setup the auto-vacuum task
        let cancellation_token = CancellationToken::new();
        tokio::task::spawn({
            let postgres_db = postgres_db.clone();
            let cancellation_token = cancellation_token.clone();
            let updatedness_requester = updatedness_requester.clone();
            async move {
                for next_time in vacuum_schedule.schedule.upcoming(vacuum_schedule.timezone) {
                    // Sleep until the next vacuum
                    let sleep_for = next_time.signed_duration_since(chrono::Utc::now());
                    let sleep_for = sleep_for
                        .to_std()
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    tokio::select! {
                        _ = tokio::time::sleep(sleep_for) => (),
                        _ = cancellation_token.cancelled() => break,
                    }

                    // Define the parameters
                    let no_new_changes_before = vacuum_schedule.recreate_older_than.map(|d| {
                        EventId(Ulid::from_parts(
                            Timestamp::from(SystemTime::now() - d).time_ms(),
                            u128::MAX,
                        ))
                    });
                    let kill_sessions_older_than = vacuum_schedule
                        .kill_sessions_older_than
                        .map(|d| Timestamp::from(SystemTime::now() - d));

                    // Retrieve the updatedness slot
                    let (sender, receiver) = oneshot::channel();
                    if let Err(_) = updatedness_requester.send(sender) {
                        tracing::error!(
                            "Updatedness request handler thread went away before autovacuum thread"
                        );
                    }
                    let Ok((updatedness, slot)) = receiver.await else {
                        tracing::error!(
                            "Updatedness request handler thread never answered autovacuum thread"
                        );
                        continue;
                    };

                    // Finally, run the vacuum
                    let mut updates = Vec::new();
                    if let Err(err) = postgres_db
                        .vacuum(
                            no_new_changes_before,
                            updatedness,
                            kill_sessions_older_than,
                            |update| updates.push(update),
                        )
                        .await
                    {
                        tracing::error!(?err, "scheduled vacuum failed");
                    }
                    if let Err(_) = slot.send(Updates {
                        data: updates,
                        now_have_all_until: updatedness,
                    }) {
                        tracing::error!("Update reorderer went away before autovacuum");
                    }
                }
            }
        });

        // Finally, return the information
        let this = Server {
            _cache_db: cache_db,
            postgres_db,
            updatedness_requester,
            _cleanup_token: cancellation_token.drop_guard(),
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
        let (updatedness, slot) = self.updatedness_slot().await?;
        let mut updates = Vec::new();
        let res = self
            .postgres_db
            .vacuum(
                no_new_changes_before,
                updatedness,
                kill_sessions_older_than,
                |update| updates.push(update),
            )
            .await;
        if let Err(_) = slot.send(Updates {
            data: updates,
            now_have_all_until: updatedness,
        }) {
            tracing::error!("Update reorderer went away before server");
        }
        res
    }

    async fn updatedness_slot(&self) -> crate::Result<(Updatedness, oneshot::Sender<Updates>)> {
        let (sender, receiver) = oneshot::channel();
        self.updatedness_requester.send(sender).map_err(|_| {
            crate::Error::Other(anyhow!(
                "Updatedness request handler thread went away too early"
            ))
        })?;
        let slot = receiver.await.map_err(|_| {
            crate::Error::Other(anyhow!("Updatedness request handler thread never answered"))
        })?;
        Ok(slot)
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
