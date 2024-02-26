use anyhow::anyhow;
use axum::extract::ws::{self, WebSocket};
use crdb_cache::CacheDb;
use crdb_core::{
    BinPtr, ClientMessage, Db, EventId, MaybeObject, MaybeSnapshot, ObjectId, Query, QueryId,
    ReadPermsChanges, Request, RequestId, ResponsePart, ResultExt, ServerMessage, ServerSideDb,
    Session, SessionRef, SessionToken, SystemTimeExt, Update, UpdateData, Updatedness, Updates,
    UpdatesWithSnap, Upload, User,
};
use futures::{
    future::{self, Either, OptionFuture},
    pin_mut, stream, FutureExt, StreamExt,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

pub use chrono;
pub use cron;
pub use sqlx;

pub use crdb_core::{Error, Result};
pub use crdb_postgres::PostgresDb;

pub type UserUpdatesMap = HashMap<ObjectId, Arc<UpdatesWithSnap>>;

pub type UpdatesMap = HashMap<User, Arc<UserUpdatesMap>>;

type EditableUpdatesMap = HashMap<User, HashMap<ObjectId, Arc<UpdatesWithSnap>>>;

type SessionsSenderMap = HashMap<
    User,
    HashMap<SessionRef, Vec<mpsc::UnboundedSender<(Updatedness, Arc<UserUpdatesMap>)>>>,
>;

pub struct Server<C: crdb_core::Config> {
    db: Arc<CacheDb<PostgresDb<C>>>,
    last_completed_updatedness: Arc<Mutex<Updatedness>>,
    updatedness_requester:
        mpsc::UnboundedSender<oneshot::Sender<(Updatedness, oneshot::Sender<UpdatesMap>)>>,
    _cleanup_token: tokio_util::sync::DropGuard,
    sessions: Arc<Mutex<SessionsSenderMap>>,
}

impl<C: crdb_core::Config> Server<C> {
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
        C::check_ulids();

        // Connect to the database and setup the cache
        let db = PostgresDb::connect(db, cache_watermark).await?;

        // Immediately update the permissions of objects pending permissions upgrades
        // This must happen before starting the server, so long as we do not actually push the returned ReadPermsChange's to subscribers
        db.update_pending_rdeps()
            .await
            .wrap_context("updating all pending reverse-dependencies")?;

        // Start the upgrading task
        let upgrade_handle = tokio::task::spawn({
            let db = db.clone();
            async move { C::reencode_old_versions(&*db).await }
        });

        // Setup the update reorderer task
        let (updatedness_requester, mut updatedness_request_receiver) = mpsc::unbounded_channel::<
            oneshot::Sender<(Updatedness, oneshot::Sender<UpdatesMap>)>,
        >();
        let (update_sender, mut update_receiver) = mpsc::unbounded_channel();
        let last_completed_updatedness = Arc::new(Mutex::new(Updatedness::from_u128(0)));
        tokio::task::spawn(async move {
            // Updatedness request handler
            let mut generator = ulid::Generator::new();
            // No cancellation token needed, closing the sender will naturally close this task
            while let Some(requester) = updatedness_request_receiver.recv().await {
                // TODO(blocked): use generate_overflowing once it lands https://github.com/dylanhart/ulid-rs/pull/75
                let updatedness = Updatedness(generator.generate().expect(
                    "you're either very unlucky, or generated 2**80 updates within one millisecond",
                ));
                let (sender, receiver) = oneshot::channel();
                if update_sender.send((updatedness, receiver)).is_err() {
                    tracing::error!(
                        "Update reorderer task went away before updatedness request handler task"
                    );
                }
                let _ = requester.send((updatedness, sender)); // Ignore any failures, they'll free the slot anyway
            }
        });
        let sessions = Arc::new(Mutex::new(SessionsSenderMap::new()));
        tokio::task::spawn({
            let sessions = sessions.clone();
            let last_completed_updatedness = last_completed_updatedness.clone();
            async move {
                // Actual update reorderer
                // No cancellation token needed, closing the senders will naturally close this task
                while let Some((updatedness, update_receiver)) = update_receiver.recv().await {
                    // Ignore the case where the slot sender was dropped
                    if let Ok(updates) = update_receiver.await {
                        let mut sessions = sessions.lock().unwrap();
                        for (user, updates) in updates {
                            if let Some(sessions) = sessions.get_mut(&user) {
                                for senders in sessions.values_mut() {
                                    // Discard all senders that return an error
                                    senders.retain(|sender| {
                                        sender.send((updatedness, updates.clone())).is_ok()
                                    });
                                    // TODO(perf-med): remove the entry from the hashmap altogether if it becomes empty
                                }
                            }
                        }
                    }
                    *last_completed_updatedness.lock().unwrap() = updatedness;
                }
            }
        });

        // Setup the auto-vacuum task
        let cancellation_token = CancellationToken::new();
        tokio::task::spawn({
            let db = db.clone();
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
                            (SystemTime::now() - d).ms_since_posix().unwrap() as u64,
                            u128::MAX,
                        ))
                    });
                    let kill_sessions_older_than = vacuum_schedule
                        .kill_sessions_older_than
                        .map(|d| SystemTime::now() - d);

                    // Retrieve the updatedness slot
                    let (sender, receiver) = oneshot::channel();
                    if updatedness_requester.send(sender).is_err() {
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
                        &db,
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
            db,
            last_completed_updatedness,
            updatedness_requester,
            _cleanup_token: cancellation_token.drop_guard(),
            sessions,
        };
        Ok((this, upgrade_handle))
    }

    pub async fn login_session(
        &self,
        user_id: User,
        session_name: String,
        expiration_time: Option<SystemTime>,
    ) -> crate::Result<(SessionToken, SessionRef)> {
        let now = SystemTime::now();
        self.db
            .login_session(Session {
                user_id,
                session_ref: SessionRef::now(),
                session_name,
                login_time: now,
                last_active: now,
                expiration_time,
            })
            .await
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
                    Some(Ok(ws::Message::Binary(bin))) => {
                        if let Err(err) = self.handle_client_binary(&mut conn, bin.into_boxed_slice().into()).await {
                            tracing::warn!(?err, "client binary violated protocol");
                            break;
                        }
                    }
                },

                Some(update) = OptionFuture::from(conn.session.as_mut().map(|s| s.updates_receiver.recv())) => {
                    let Some((updatedness, update)) = update else {
                        tracing::error!("Update receiver broke before connection went down");
                        break;
                    };
                    let sess = conn.session.as_ref().unwrap();
                    // TODO(perf-high): make sure the size of update messages is both as batched as possible and not too big
                    let mut data = Vec::new();
                    for (object_id, updates) in update.iter() {
                        if sess.is_subscribed_to(*object_id, updates.new_last_snapshot.as_deref()) {
                            data.extend(updates.updates.iter().cloned());
                        }
                    }
                    let send_res = Self::send(&mut conn.socket, &ServerMessage::Updates(Updates {
                        data,
                        now_have_all_until: updatedness,
                    })).await;
                    if let Err(err) = send_res {
                        tracing::warn!(?err, "failed sending update to client");
                        break;
                    }
                },
            }
        }
    }

    async fn handle_client_binary(
        &self,
        conn: &mut ConnectionState,
        bin: Arc<[u8]>,
    ) -> crate::Result<()> {
        // Check we're waiting for binaries and count one as done
        {
            let sess = conn
                .session
                .as_mut()
                .ok_or(crate::Error::ProtocolViolation)?;
            sess.expected_binaries = sess
                .expected_binaries
                .checked_sub(1)
                .ok_or(crate::Error::ProtocolViolation)?;
        }

        // Actually send the binary
        let binary_id = crdb_core::hash_binary(&bin);
        self.db.create_binary(binary_id, bin).await
    }

    async fn handle_client_message(
        &self,
        conn: &mut ConnectionState,
        msg: &str,
    ) -> crate::Result<()> {
        if conn
            .session
            .as_ref()
            .map(|sess| sess.expected_binaries > 0)
            .unwrap_or(false)
        {
            return Err(crate::Error::ProtocolViolation);
        }
        tracing::trace!(?msg, session=?conn.session.as_ref().map(|s| &s.session), "received client message");
        if let Some(sess) = &conn.session {
            // TODO(perf-low): do not mark the session as active upon each incoming message? we don't really need to
            // mark the session as active every 10 seconds / 1 minute, yet that's the frequency at which the clients
            // send GetTime in order to detect disconnection
            self.db
                .mark_session_active(sess.token, SystemTime::now())
                .await
                .wrap_context("marking session as active")?;
        }
        let msg = serde_json::from_str::<ClientMessage>(msg)
            .wrap_context("deserializing client message")?;
        // TODO(perf-med): We could parallelize requests here, and not just pipeline them. However, we need to be
        // careful about not sending updates about subscribed objects before the objects themselves, so it is
        // nontrivial. Do this only after thinking well about what could happen.
        match &*msg.request {
            Request::SetToken(token) => {
                let res = self.db.resume_session(*token).await.map(|session| {
                    let (updates_sender, updates_receiver) = mpsc::unbounded_channel();
                    self.sessions
                        .lock()
                        .unwrap()
                        .entry(session.user_id)
                        .or_default()
                        .entry(session.session_ref)
                        .or_default()
                        .push(updates_sender);
                    conn.session = Some(SessionInfo {
                        token: *token,
                        session,
                        expected_binaries: 0,
                        subscribed_objects: Arc::new(RwLock::new(HashSet::new())),
                        subscribed_queries: Arc::new(RwLock::new(HashMap::new())),
                        updates_receiver,
                    });
                    ResponsePart::Success
                });
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::RenameSession(name) => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .db
                        .rename_session(sess.token, name)
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
                        .db
                        .list_sessions(sess.session.user_id)
                        .await
                        .map(ResponsePart::Sessions),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::Logout => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .db
                        .disconnect_session(sess.session.user_id, sess.session.session_ref)
                        .await
                        .map(|()| ResponsePart::Success),
                };
                conn.session = None;
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::DisconnectSession(session_ref) => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(sess) => self
                        .db
                        .disconnect_session(sess.session.user_id, *session_ref)
                        .await
                        .map(|()| ResponsePart::Success),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::GetTime => {
                let res = match &conn.session {
                    None => Err(crate::Error::ProtocolViolation),
                    Some(_) => Ok(ResponsePart::CurrentTime(SystemTime::now())),
                };
                Self::send_res(&mut conn.socket, msg.request_id, res).await
            }
            Request::GetSubscribe(object_ids) => {
                // TODO(blocked): remove this copy once https://github.com/rust-lang/rust/issues/110338 is fixed
                let object_ids = object_ids.iter().map(|(o, u)| (*o, *u)).collect::<Vec<_>>();
                self.send_objects(conn, msg.request_id, None, object_ids.into_iter())
                    .await
            }
            Request::QuerySubscribe {
                query_id,
                type_id,
                query,
                only_updated_since,
            } => {
                let sess = conn
                    .session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?;
                // Subscribe BEFORE running the query. This makes sure no updates are lost.
                // We must then not return to the update-sending loop until all the responses are sent.
                sess.subscribed_queries
                    .write()
                    .unwrap()
                    .insert(*query_id, query.clone());
                let updatedness = *self.last_completed_updatedness.lock().unwrap();
                let object_ids = self
                    .db
                    .query(
                        sess.session.user_id,
                        *type_id,
                        *only_updated_since,
                        query.clone(),
                    )
                    .await
                    .wrap_context("listing objects matching query")?;
                // Note: `send_objects` will only fetch and send objects that the user has not yet subscribed upon.
                // So, setting `None` here is the right thing to do.
                self.send_objects(
                    conn,
                    msg.request_id,
                    Some(updatedness),
                    object_ids.into_iter().map(|o| (o, None)),
                )
                .await
            }
            Request::GetLatest(object_ids) => {
                self.send_snapshots(conn, msg.request_id, None, object_ids.iter().copied())
                    .await
            }
            Request::QueryLatest {
                type_id,
                query,
                only_updated_since,
            } => {
                let sess = conn
                    .session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?;
                let updatedness = *self.last_completed_updatedness.lock().unwrap();
                let object_ids = self
                    .db
                    .query(
                        sess.session.user_id,
                        *type_id,
                        *only_updated_since,
                        query.clone(),
                    )
                    .await
                    .wrap_context("listing objects matching query")?;
                self.send_snapshots(
                    conn,
                    msg.request_id,
                    Some(updatedness),
                    object_ids.into_iter(),
                )
                .await
            }
            Request::GetBinaries(binary_ids) => {
                // Just avoid unauthed binary gets
                let _ = conn
                    .session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?;
                self.send_binaries(conn, msg.request_id, binary_ids.iter().copied())
                    .await
            }
            Request::Unsubscribe(object_ids) => {
                {
                    let mut subscribed_objects = conn
                        .session
                        .as_ref()
                        .ok_or(crate::Error::ProtocolViolation)?
                        .subscribed_objects
                        .write()
                        .unwrap();
                    for id in object_ids {
                        subscribed_objects.remove(id);
                    }
                }
                Self::send_res(&mut conn.socket, msg.request_id, Ok(ResponsePart::Success)).await
            }
            Request::UnsubscribeQuery(query_id) => {
                conn.session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?
                    .subscribed_queries
                    .write()
                    .unwrap()
                    .remove(query_id);
                Self::send_res(&mut conn.socket, msg.request_id, Ok(ResponsePart::Success)).await
            }
            Request::Upload(upload) => {
                let sess = conn
                    .session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?;
                match upload {
                    Upload::Object {
                        object_id,
                        type_id,
                        created_at,
                        snapshot_version,
                        object,
                        subscribe,
                    } => {
                        let (updatedness, update_sender) = self.updatedness_slot().await?;
                        let res = C::upload_object(
                            &**self.db,
                            sess.session.user_id,
                            updatedness,
                            *type_id,
                            *object_id,
                            *created_at,
                            *snapshot_version,
                            object.clone(),
                            &*self.db,
                        )
                        .await;
                        let res = match res {
                            Ok(res) => res,
                            Err(err) => {
                                return Self::send_res(&mut conn.socket, msg.request_id, Err(err))
                                    .await;
                            }
                        };
                        if let Some((new_update, users_who_can_read, rdeps)) = res {
                            let mut new_data = HashMap::new();
                            self.add_rdeps_updates(&mut new_data, rdeps)
                                .await
                                .wrap_context("listing updates for rdeps")?;
                            for user in users_who_can_read {
                                let existing = new_data
                                    .entry(user)
                                    .or_insert_with(HashMap::new)
                                    .insert(*object_id, new_update.clone());
                                if let Some(existing) = existing {
                                    tracing::error!(
                                        ?user,
                                        ?object_id,
                                        ?existing,
                                        "replacing mistakenly-already-existing update"
                                    );
                                }
                            }
                            let new_data = new_data
                                .into_iter()
                                .map(|(k, v)| (k, Arc::new(v)))
                                .collect();

                            update_sender.send(new_data).map_err(|_| {
                                crate::Error::Other(anyhow!(
                                    "Update reorderer thread went away before updating thread",
                                ))
                            })?;
                        }
                        if *subscribe {
                            sess.subscribed_objects.write().unwrap().insert(*object_id);
                        }
                        Self::send_res(&mut conn.socket, msg.request_id, Ok(ResponsePart::Success))
                            .await
                    }
                    Upload::Event {
                        object_id,
                        type_id,
                        event_id,
                        event,
                        subscribe,
                    } => {
                        let (updatedness, update_sender) = self.updatedness_slot().await?;
                        let res = C::upload_event(
                            &**self.db,
                            sess.session.user_id,
                            updatedness,
                            *type_id,
                            *object_id,
                            *event_id,
                            event.clone(),
                            &*self.db,
                        )
                        .await;
                        let res = match res {
                            Ok(res) => res,
                            Err(err) => {
                                return Self::send_res(&mut conn.socket, msg.request_id, Err(err))
                                    .await;
                            }
                        };
                        if let Some((new_update, users_who_can_read, rdeps)) = res {
                            let mut new_data = HashMap::new();
                            self.add_rdeps_updates(&mut new_data, rdeps)
                                .await
                                .wrap_context("listing updates for rdeps")?;
                            for user in users_who_can_read {
                                let existing = new_data
                                    .entry(user)
                                    .or_insert_with(HashMap::new)
                                    .insert(*object_id, new_update.clone());
                                if let Some(existing) = existing {
                                    tracing::error!(
                                        ?user,
                                        ?object_id,
                                        ?existing,
                                        "replacing mistakenly-already-existing update"
                                    );
                                }
                            }
                            let new_data = new_data
                                .into_iter()
                                .map(|(k, v)| (k, Arc::new(v)))
                                .collect();

                            update_sender.send(new_data).map_err(|_| {
                                crate::Error::Other(anyhow!(
                                    "Update reorderer thread went away before updating thread",
                                ))
                            })?;
                        }
                        if *subscribe {
                            sess.subscribed_objects.write().unwrap().insert(*object_id);
                        }
                        Self::send_res(&mut conn.socket, msg.request_id, Ok(ResponsePart::Success))
                            .await
                    }
                }
            }
            Request::UploadBinaries(num_binaries) => {
                conn.session
                    .as_mut()
                    .ok_or(crate::Error::ProtocolViolation)?
                    .expected_binaries = *num_binaries;
                Self::send_res(&mut conn.socket, msg.request_id, Ok(ResponsePart::Success)).await
            }
        }
    }

    async fn add_rdeps_updates(
        &self,
        updates: &mut EditableUpdatesMap,
        rdeps: Vec<ReadPermsChanges>,
    ) -> crate::Result<()> {
        for c in rdeps {
            for u in c.lost_read {
                // TODO(api-highest): currently, losing and then re-gaining read rights on a query-subscribed object does
                // not result in a full retransmit, but just in an update, which thus gets ignored because the client doesn't
                // have the local object any longer
                updates.entry(u).or_default().insert(
                    c.object_id,
                    Arc::new(UpdatesWithSnap {
                        updates: vec![Arc::new(Update {
                            object_id: c.object_id,
                            data: UpdateData::LostReadRights,
                        })],
                        new_last_snapshot: None,
                    }),
                );
            }
            if let Some(one_user) = c.gained_read.iter().next() {
                let mut t = self.db.get_transaction().await?;
                let object = self
                    .db
                    .get_all(&mut t, *one_user, c.object_id, None)
                    .await?;
                let last_snapshot = self
                    .db
                    .get_latest_snapshot(&mut t, *one_user, c.object_id)
                    .await?;
                let new_updates = object.into_updates();
                for u in c.gained_read {
                    updates.entry(u).or_default().insert(
                        c.object_id,
                        Arc::new(UpdatesWithSnap {
                            updates: new_updates.clone(),
                            new_last_snapshot: Some(last_snapshot.snapshot.clone()),
                        }),
                    );
                }
            }
        }
        Ok(())
    }

    async fn send_objects(
        &self,
        conn: &mut ConnectionState,
        request_id: RequestId,
        query_updatedness: Option<Updatedness>,
        objects: impl Iterator<Item = (ObjectId, Option<Updatedness>)>,
    ) -> crate::Result<()> {
        let sess = conn
            .session
            .as_ref()
            .ok_or(crate::Error::ProtocolViolation)?;
        let user = sess.session.user_id;
        let subscribed_objects = sess.subscribed_objects.clone();
        let objects = objects.map(|(object_id, updatedness)| {
            let subscribed_objects = subscribed_objects.clone();
            async move {
                if subscribed_objects.read().unwrap().contains(&object_id) {
                    Ok(MaybeObject::AlreadySubscribed(object_id))
                } else {
                    // Subscribe BEFORE getting the object. This makes sure no updates are lost.
                    // We must then not return to the update-sending loop until all the responses are sent.
                    subscribed_objects.write().unwrap().insert(object_id);
                    let mut t = self.db.get_transaction().await?;
                    let object = self
                        .db
                        .get_all(&mut t, user, object_id, updatedness)
                        .await?;
                    Ok(MaybeObject::NotYetSubscribed(object))
                }
            }
        });
        let objects = stream::iter(objects).buffer_unordered(16); // TODO(perf-low): is 16 a good number?
        pin_mut!(objects);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the objects to the client, batching them by messages of a reasonable size, to both allow for better
        // resumption after a connection loss, while not sending one message per mini-object.
        while let Some(object) = objects.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(perf-low): is 1MiB a good number?
                let data = std::mem::take(&mut current_data);
                size_of_message = 0;
                Self::send(
                    &mut conn.socket,
                    &ServerMessage::Response {
                        request_id,
                        response: ResponsePart::Objects {
                            data,
                            now_have_all_until: None,
                        },
                        last_response: false,
                    },
                )
                .await?;
            }
            match object {
                Ok(object) => {
                    size_of_message += size_as_json(&object)?;
                    current_data.push(object);
                }
                Err(err @ crate::Error::ObjectDoesNotExist(_)) => {
                    if query_updatedness.is_some() {
                        // User lost read access to object between query and read
                        // Do nothing
                    } else {
                        // User explicitly requested a non-existing object
                        // Return an error but keep processing the request
                        Self::send(
                            &mut conn.socket,
                            &ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Error(err.into()),
                                last_response: false,
                            },
                        )
                        .await?;
                    }
                }
                Err(err) => return Self::send_res(&mut conn.socket, request_id, Err(err)).await,
            }
        }
        Self::send_res(
            &mut conn.socket,
            request_id,
            Ok(ResponsePart::Objects {
                data: current_data,
                now_have_all_until: query_updatedness,
            }),
        )
        .await
    }

    async fn send_snapshots(
        &self,
        conn: &mut ConnectionState,
        request_id: RequestId,
        query_updatedness: Option<Updatedness>,
        object_ids: impl Iterator<Item = ObjectId>,
    ) -> crate::Result<()> {
        let sess = conn
            .session
            .as_ref()
            .ok_or(crate::Error::ProtocolViolation)?;
        let user = sess.session.user_id;
        let snapshots = object_ids.map(|object_id| {
            if sess.subscribed_objects.read().unwrap().contains(&object_id) {
                Either::Left(future::ready(Ok(MaybeSnapshot::AlreadySubscribed(
                    object_id,
                ))))
            } else {
                Either::Right(async move {
                    let mut t = self.db.get_transaction().await?;
                    let snapshot = self.db.get_latest_snapshot(&mut t, user, object_id).await?;
                    Ok(MaybeSnapshot::NotSubscribed(snapshot))
                })
            }
        });
        let snapshots = stream::iter(snapshots).buffer_unordered(16); // TODO(perf-low): is 16 a good number?
        pin_mut!(snapshots);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the snapshots to the client, batching them by messages of a reasonable size, to both allow for better
        // resumption after a connection loss, while not sending one message per mini-object.
        while let Some(snapshot) = snapshots.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(perf-low): is 1MiB a good number?
                let data = std::mem::take(&mut current_data);
                size_of_message = 0;
                Self::send(
                    &mut conn.socket,
                    &ServerMessage::Response {
                        request_id,
                        response: ResponsePart::Snapshots {
                            data,
                            now_have_all_until: None,
                        },
                        last_response: false,
                    },
                )
                .await?;
            }
            match snapshot {
                Ok(snapshot) => {
                    size_of_message += size_as_json(&snapshot)?;
                    current_data.push(snapshot);
                }
                Err(err @ crate::Error::ObjectDoesNotExist(_)) => {
                    if query_updatedness.is_some() {
                        // User lost read access to object between query and read
                        // Do nothing
                    } else {
                        // User explicitly requested a non-existing object
                        // Return an error but keep processing the request
                        Self::send(
                            &mut conn.socket,
                            &ServerMessage::Response {
                                request_id,
                                response: ResponsePart::Error(err.into()),
                                last_response: false,
                            },
                        )
                        .await?;
                    }
                }
                Err(err) => return Self::send_res(&mut conn.socket, request_id, Err(err)).await,
            };
        }
        Self::send_res(
            &mut conn.socket,
            request_id,
            Ok(ResponsePart::Snapshots {
                data: current_data,
                now_have_all_until: query_updatedness,
            }),
        )
        .await
    }

    async fn send_binaries(
        &self,
        conn: &mut ConnectionState,
        request_id: RequestId,
        binaries: impl Iterator<Item = BinPtr>,
    ) -> crate::Result<()> {
        let binaries =
            binaries.map(|binary_id| self.db.get_binary(binary_id).map(move |r| (binary_id, r)));
        let binaries = stream::iter(binaries).buffer_unordered(16); // TODO(perf-low): is 16 a good number?
        pin_mut!(binaries);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the binaries to the client, trying to avoid having too many ResponsePart::Binaries messages while still sending as
        // many binaries as possible before any potential error (in particular missing-binary).
        while let Some((binary_id, binary)) = binaries.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(perf-low): is 1MiB a good number?
                size_of_message = 0;
                Self::send_binaries_msg(
                    &mut conn.socket,
                    request_id,
                    false,
                    current_data.drain(..),
                )
                .await?;
            }
            let binary = match binary {
                Ok(Some(binary)) => Ok(binary),
                Ok(None) => Err(crate::Error::MissingBinaries(vec![binary_id])),
                Err(err) => Err(err),
            };
            let binary = match binary {
                Ok(binary) => binary,
                Err(err) => {
                    if !current_data.is_empty() {
                        Self::send_binaries_msg(
                            &mut conn.socket,
                            request_id,
                            false,
                            current_data.drain(..),
                        )
                        .await?;
                    }
                    return Self::send_res(&mut conn.socket, request_id, Err(err)).await;
                }
            };
            size_of_message += binary.len();
            current_data.push(binary);
        }
        Self::send_binaries_msg(&mut conn.socket, request_id, true, current_data.drain(..)).await
    }

    async fn send_binaries_msg(
        socket: &mut WebSocket,
        request_id: RequestId,
        last_response: bool,
        bins: impl ExactSizeIterator<Item = Arc<[u8]>>,
    ) -> crate::Result<()> {
        Self::send(
            socket,
            &ServerMessage::Response {
                request_id,
                last_response,
                response: ResponsePart::Binaries(bins.len()),
            },
        )
        .await?;
        for bin in bins {
            socket
                .send(ws::Message::Binary(bin.to_vec()))
                .await
                .wrap_context("sending binary to client")?
        }
        Ok(())
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
        kill_sessions_older_than: Option<SystemTime>,
    ) -> crate::Result<()> {
        let (updatedness, slot) = self.updatedness_slot().await?;
        Self::run_vacuum(
            &self.db,
            no_new_changes_before,
            updatedness,
            kill_sessions_older_than,
            slot,
        )
        .await
    }

    async fn run_vacuum(
        db: &CacheDb<PostgresDb<C>>,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<SystemTime>,
        slot: oneshot::Sender<UpdatesMap>,
    ) -> crate::Result<()> {
        // Perform the vacuum, collecting all updates
        let mut updates = HashMap::new();
        let res = db
            .server_vacuum(
                no_new_changes_before,
                updatedness,
                kill_sessions_older_than,
                |update, users_who_can_read| {
                    // Vacuum cannot change any latest snapshot
                    let object_id = update.object_id;
                    let update = Arc::new(UpdatesWithSnap {
                        updates: vec![Arc::new(update)],
                        new_last_snapshot: None,
                    });
                    for u in users_who_can_read {
                        updates
                            .entry(u)
                            .or_insert_with(HashMap::new)
                            .insert(object_id, update.clone());
                    }
                },
            )
            .await;

        // Arc where appropriate
        // TODO(perf-low): this could probably be done without copying by having &muts to the underlying Arc at creation time
        let updates = updates.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();

        // Submit the updates
        if slot.send(updates).is_err() {
            tracing::error!("Update reorderer went away before server");
        }

        // And return the result
        res
    }

    async fn updatedness_slot(&self) -> crate::Result<(Updatedness, oneshot::Sender<UpdatesMap>)> {
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
    expected_binaries: usize,
    subscribed_objects: Arc<RwLock<HashSet<ObjectId>>>,
    subscribed_queries: Arc<RwLock<HashMap<QueryId, Arc<Query>>>>,
    updates_receiver: mpsc::UnboundedReceiver<(Updatedness, Arc<UserUpdatesMap>)>,
}

impl SessionInfo {
    fn is_subscribed_to(
        &self,
        object_id: ObjectId,
        new_last_snapshot: Option<&serde_json::Value>,
    ) -> bool {
        if self.subscribed_objects.read().unwrap().contains(&object_id) {
            return true;
        }
        if let Some(new_last_snapshot) = new_last_snapshot {
            for query in self.subscribed_queries.read().unwrap().values() {
                if query.matches_json(new_last_snapshot) {
                    self.subscribed_objects.write().unwrap().insert(object_id);
                    return true;
                }
            }
        }
        false
    }
}

fn size_as_json<T: serde::Serialize>(value: &T) -> crate::Result<usize> {
    struct Size(usize);
    impl std::io::Write for Size {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut size = Size(0);
    serde_json::to_writer(&mut size, value)
        .wrap_context("figuring out the serialized size of value")?;
    Ok(size.0)
}
