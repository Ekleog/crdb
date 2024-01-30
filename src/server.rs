use crate::{
    api::ApiConfig,
    cache::CacheDb,
    crdb_internal::ResultExt,
    messages::{ClientMessage, Request, RequestId, ResponsePart, ServerMessage, Updates},
    EventId, Session, SessionRef, SessionToken, Timestamp, Updatedness, User,
};
use anyhow::anyhow;
use axum::extract::ws::{self, WebSocket};
use futures::{future::OptionFuture, StreamExt};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
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

// Each update is both the list of updates itself, and the new latest snapshot
// for query matching, available if the latest snapshot actually changed. Also,
// the list of users allowed to read this object.
type UpdatesWithSnap = Arc<(Updates, Option<serde_json::Value>, Vec<User>)>;

pub struct Server<C: ServerConfig> {
    _cache_db: Arc<CacheDb<PostgresDb<C>>>, // TODO(api): use this to implement the server
    postgres_db: Arc<PostgresDb<C>>,
    updatedness_requester: mpsc::UnboundedSender<
        oneshot::Sender<(Updatedness, mpsc::UnboundedSender<UpdatesWithSnap>)>,
    >,
    _cleanup_token: tokio_util::sync::DropGuard,
    sessions:
        Arc<Mutex<HashMap<User, HashMap<SessionRef, Vec<mpsc::UnboundedSender<UpdatesWithSnap>>>>>>,
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
        let (updatedness_requester, mut updatedness_request_receiver) = mpsc::unbounded_channel::<
            oneshot::Sender<(Updatedness, mpsc::UnboundedSender<UpdatesWithSnap>)>,
        >();
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
                let _ = requester.send((updatedness, sender)); // Ignore any failures, they'll free the slot anyway
            }
        });
        let sessions = Arc::new(Mutex::new(HashMap::<
            User,
            HashMap<SessionRef, Vec<mpsc::UnboundedSender<UpdatesWithSnap>>>,
        >::new()));
        tokio::task::spawn({
            let sessions = sessions.clone();
            async move {
                // Actual update reorderer
                // No cancellation token needed, closing the senders will naturally close this task
                while let Some(mut update_receiver) = update_receiver.recv().await {
                    // Ignore the case where the slot sender was dropped
                    while let Some(updates) = update_receiver.recv().await {
                        let mut sessions = sessions.lock().unwrap();
                        for user in updates.2.iter() {
                            if let Some(sessions) = sessions.get_mut(user) {
                                for senders in sessions.values_mut() {
                                    // Discard all senders that return an error
                                    senders.retain(|sender| sender.send(updates.clone()).is_ok());
                                    // TODO(low): remove the entry from the hashmap altogether if it becomes empty
                                }
                            }
                        }
                    }
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
                    if let Err(err) = Self::run_vacuum(
                        &postgres_db,
                        no_new_changes_before,
                        updatedness,
                        kill_sessions_older_than,
                        slot,
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
            sessions,
        };
        Ok((this, upgrade_handle))
    }

    pub async fn answer(&self, socket: WebSocket) {
        let mut conn = ConnectionState {
            socket,
            session: None,
        };
        loop {
            tokio::select! {
                msg = conn.socket.next() => match msg {
                    None => break, // End-of-stream
                    Some(Err(err)) => {
                        tracing::warn!(?err, "received an error while waiting for message on websocket");
                        break;
                    }
                    Some(Ok(ws::Message::Ping(_) | ws::Message::Pong(_))) => continue, // Auto-handled by axum, ignore
                    Some(Ok(ws::Message::Close(_))) => break, // End-of-stream
                    Some(Ok(ws::Message::Text(msg))) => {
                        if let Err(err) = self.handle_client_message(&mut conn, &msg).await {
                            tracing::warn!(?err, ?msg, "client message violated protocol");
                            break;
                        }
                    }
                    Some(Ok(ws::Message::Binary(_msg))) => {
                        unimplemented!() // TODO(api)
                    }
                },

                Some(_msg) = OptionFuture::from(conn.session.as_mut().map(|s| s.updates_receiver.recv())) => {
                    unimplemented!() // TODO(api)
                },
            }
        }
    }

    async fn handle_client_message(
        &self,
        conn: &mut ConnectionState,
        msg: &str,
    ) -> crate::Result<()> {
        let msg = serde_json::from_str::<ClientMessage>(msg)
            .wrap_context("deserializing client message")?;
        match &*msg.request {
            Request::SetToken(token) => {
                let res = self
                    .postgres_db
                    .resume_session(*token)
                    .await
                    .map(|session| {
                        let (updates_sender, updates_receiver) = mpsc::unbounded_channel();
                        self.sessions
                            .lock()
                            .unwrap()
                            .entry(session.user_id)
                            .or_insert_with(HashMap::new)
                            .entry(session.session_ref)
                            .or_insert_with(Vec::new)
                            .push(updates_sender);
                        conn.session = Some(SessionInfo {
                            token: *token,
                            session,
                            updates_receiver,
                        });
                        ResponsePart::Success
                    });
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            // TODO(client): expose RenameSession & co to end-user
            Request::RenameSession(name) => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .postgres_db
                        .rename_session(sess.token, &name)
                        .await
                        .map(|()| ResponsePart::Success),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::CurrentSession => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => Ok(ResponsePart::Sessions(vec![sess.session.clone()])),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::ListSessions => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .postgres_db
                        .list_sessions(sess.session.user_id)
                        .await
                        .map(ResponsePart::Sessions),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::DisconnectSession(session_ref) => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .postgres_db
                        .disconnect_session(sess.session.user_id, *session_ref)
                        .await
                        .map(|()| ResponsePart::Success),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::GetTime => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(_) => Ok(ResponsePart::CurrentTime(Timestamp::now())),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            // TODO(api): when implementing Get, make sure to:
            // 1. subscribe the user to the object ids, to not miss events between the initial fetch and the subscription
            // 2. make sure not to return early to the `answer` loop! (and document why). This avoids Updates being sent too early
            _ => {
                unimplemented!() // TODO(api)
            }
        }
    }

    async fn send_res(
        socket: &mut WebSocket,
        request_id: RequestId,
        res: crate::Result<ResponsePart>,
    ) -> crate::Result<()> {
        let response = match res {
            Ok(res) => res,
            Err(err) => ResponsePart::Error(err.into()),
        };
        Self::send(
            socket,
            &ServerMessage::Response {
                request_id,
                response,
                last_response: true,
            },
        )
        .await
    }

    async fn send(socket: &mut WebSocket, msg: &ServerMessage) -> crate::Result<()> {
        let msg = serde_json::to_string(msg).wrap_context("serializing server message")?;
        socket
            .send(ws::Message::Text(msg))
            .await
            .wrap_context("sending response to client")
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
        Self::run_vacuum(
            &self.postgres_db,
            no_new_changes_before,
            updatedness,
            kill_sessions_older_than,
            slot,
        )
        .await
    }

    async fn run_vacuum(
        postgres_db: &PostgresDb<C>,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<Timestamp>,
        slot: mpsc::UnboundedSender<Arc<(Updates, Option<serde_json::Value>, Vec<User>)>>,
    ) -> crate::Result<()> {
        // Perform the vacuum, collecting all updates
        let mut updates = HashMap::new();
        let res = postgres_db
            .vacuum(
                no_new_changes_before,
                updatedness,
                kill_sessions_older_than,
                |update, users_who_can_read| {
                    for u in users_who_can_read {
                        updates
                            .entry(u)
                            .or_insert_with(Vec::new)
                            .push(update.clone());
                    }
                },
            )
            .await;

        // Submit the updates
        for (user, updates) in updates {
            // Vacuum cannot change any latest snapshot
            if let Err(_) = slot.send(Arc::new((
                Updates {
                    data: updates,
                    now_have_all_until: updatedness,
                },
                None,
                vec![user],
            ))) {
                tracing::error!("Update reorderer went away before server");
            }
        }

        // And return the result
        res
    }

    async fn updatedness_slot(
        &self,
    ) -> crate::Result<(Updatedness, mpsc::UnboundedSender<UpdatesWithSnap>)> {
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

struct ConnectionState {
    socket: WebSocket,
    session: Option<SessionInfo>,
}

struct SessionInfo {
    token: SessionToken,
    session: Session,
    updates_receiver: mpsc::UnboundedReceiver<Arc<(Updates, Option<serde_json::Value>, Vec<User>)>>,
}
