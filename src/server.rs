use crate::{
    api::ApiConfig,
    cache::CacheDb,
    messages::{ServerMessage, Update},
    EventId, ObjectId, SessionToken, Timestamp, Updatedness,
};
use anyhow::{anyhow, Context};
use multimap::MultiMap;
use std::{
    collections::{HashMap, HashSet},
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

struct UpdatednessSlot {
    updatedness: Updatedness,
    sender: mpsc::UnboundedSender<Update>,
}

impl UpdatednessSlot {
    fn updatedness(&self) -> Updatedness {
        self.updatedness
    }

    fn send(&self, update: Update) {
        if let Err(_) = self.sender.send(update) {
            tracing::error!("UpdatednessSlot outlived the server!");
        }
    }
}

pub struct Server<C: ServerConfig> {
    _cache_db: Arc<CacheDb<PostgresDb<C>>>, // TODO(api): use this to implement the server
    postgres_db: Arc<PostgresDb<C>>,
    updatedness_requester: mpsc::UnboundedSender<oneshot::Sender<UpdatednessSlot>>,
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

        // Setup the update reorderer task
        let (updatedness_requester, mut updatedness_request_receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<UpdatednessSlot>>();
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
                let (sender, receiver) = mpsc::unbounded_channel();
                if let Err(_) = update_sender.send(receiver) {
                    tracing::error!(
                        "Update reorderer task went away before updatedness request handler task"
                    );
                }
                let slot = UpdatednessSlot {
                    updatedness,
                    sender,
                };
                let _ = requester.send(slot); // Ignore any failures, they'll free the slot anyway
            }
        });
        tokio::task::spawn(async move {
            // Actual update reorderer
            // No cancellation token needed, closing the senders will naturally close this task
            while let Some(mut update_receiver) = update_receiver.recv().await {
                while let Some(update) = update_receiver.recv().await {
                    let _ = update; // TODO(api): actually fan the update out to all subscribed listeners
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
                    let Ok(slot) = receiver.await else {
                        tracing::error!(
                            "Updatedness request handler thread never answered autovacuum thread"
                        );
                        continue;
                    };

                    // Finally, run the vacuum
                    if let Err(err) = postgres_db
                        .vacuum(
                            no_new_changes_before,
                            slot.updatedness(),
                            kill_sessions_older_than,
                            |update| slot.send(update),
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
            updatedness_requester,
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
        let slot = self.updatedness_slot().await?;
        self.postgres_db
            .vacuum(
                no_new_changes_before,
                slot.updatedness(),
                kill_sessions_older_than,
                |update| slot.send(update),
            )
            .await
    }

    async fn updatedness_slot(&self) -> crate::Result<UpdatednessSlot> {
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
