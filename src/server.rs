use crate::{
    api::ApiConfig,
    cache::CacheDb,
    ids::QueryId,
    messages::{
        ClientMessage, MaybeObject, MaybeSnapshot, Request, RequestId, ResponsePart, ServerMessage,
        Updates, Upload,
    },
    BinPtr, Db, EventId, ObjectId, Query, ResultExt, Session, SessionRef, SessionToken, Timestamp,
    Updatedness, User,
};
use anyhow::anyhow;
use axum::extract::ws::{self, WebSocket};
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

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

// Each update is both the list of updates itself, and the new latest snapshot
// for query matching, available if the latest snapshot actually changed. Also,
// the list of users allowed to read this object.
pub type UpdatesWithSnap = Arc<(Updates, Option<serde_json::Value>, Vec<User>)>;

pub struct Server<C: ServerConfig> {
    cache_db: Arc<CacheDb<PostgresDb<C>>>,
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
            cache_db,
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
                    Some(Ok(ws::Message::Binary(bin))) => {
                        if let Err(err) = self.handle_client_binary(&mut conn, bin.into_boxed_slice().into()).await {
                            tracing::warn!(?err, "client binary violated protocol");
                            break;
                        }
                    }
                },

                Some(_msg) = OptionFuture::from(conn.session.as_mut().map(|s| s.updates_receiver.recv())) => {
                    unimplemented!() // TODO(api)
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
        let binary_id = crate::hash_binary(&bin);
        self.postgres_db.create_binary(binary_id, bin).await
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
        let msg = serde_json::from_str::<ClientMessage>(msg)
            .wrap_context("deserializing client message")?;
        // TODO(low): We could parallelize requests here, and not just pipeline them. However, we need to be
        // careful about not sending updates about subscribed objects before the objects themselves, so it is
        // nontrivial. Do this only after thinking well about what could happen.
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
                            expected_binaries: 0,
                            subscribed_objects: Arc::new(RwLock::new(HashSet::new())),
                            subscribed_queries: Arc::new(RwLock::new(HashMap::new())),
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
            Request::GetSubscribe(object_ids) => {
                self.send_objects(
                    conn,
                    msg.request_id,
                    false,
                    object_ids.iter().map(|(o, u)| (*o, *u)),
                )
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
                let object_ids = self
                    .postgres_db
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
                    true,
                    object_ids.into_iter().map(|o| (o, None)),
                )
                .await
            }
            Request::GetLatest(object_ids) => {
                self.send_snapshots(conn, msg.request_id, false, object_ids.iter().copied())
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
                let object_ids = self
                    .postgres_db
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
                self.send_snapshots(conn, msg.request_id, true, object_ids.into_iter())
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
                Ok(())
            }
            Request::UnsubscribeQuery(query_id) => {
                conn.session
                    .as_ref()
                    .ok_or(crate::Error::ProtocolViolation)?
                    .subscribed_queries
                    .write()
                    .unwrap()
                    .remove(query_id);
                Ok(())
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
                            &*self.cache_db,
                            sess.session.user_id,
                            updatedness,
                            *type_id,
                            *object_id,
                            *created_at,
                            *snapshot_version,
                            object.clone(),
                        )
                        .await?;
                        if let Some(new_data) = res {
                            update_sender.send(new_data).map_err(|_| {
                                crate::Error::Other(anyhow!(
                                    "Update reorderer thread went away before updating thread",
                                ))
                            })?;
                        }
                        if *subscribe {
                            sess.subscribed_objects.write().unwrap().insert(*object_id);
                        }
                        Ok(())
                    }
                    Upload::Event {
                        object_id,
                        type_id,
                        event_id,
                        event,
                        subscribe,
                    } => {
                        let _ = (object_id, type_id, event_id, event, subscribe);
                        unimplemented!() // TODO(api)
                    }
                }
            }
            Request::UploadBinaries(num_binaries) => {
                conn.session
                    .as_mut()
                    .ok_or(crate::Error::ProtocolViolation)?
                    .expected_binaries = *num_binaries;
                Ok(())
            }
        }
    }

    async fn send_objects(
        &self,
        conn: &mut ConnectionState,
        request_id: RequestId,
        ignore_non_existing: bool,
        objects: impl Iterator<Item = (ObjectId, Option<Updatedness>)>,
    ) -> crate::Result<()> {
        let sess = conn
            .session
            .as_ref()
            .ok_or(crate::Error::ProtocolViolation)?;
        let user = sess.session.user_id;
        let subscribed_objects = sess.subscribed_objects.clone();
        let objects = objects.map(|(object_id, _updatedness)| {
            // TODO(server): use _updatedness as get_all parameter when possible
            let subscribed_objects = subscribed_objects.clone();
            async move {
                if subscribed_objects.read().unwrap().contains(&object_id) {
                    Ok(MaybeObject::AlreadySubscribed(object_id))
                } else {
                    // Subscribe BEFORE getting the object. This makes sure no updates are lost.
                    // We must then not return to the update-sending loop until all the responses are sent.
                    subscribed_objects.write().unwrap().insert(object_id);
                    let object = self.postgres_db.get_all(user, object_id).await?;
                    Ok(MaybeObject::NotYetSubscribed(object))
                }
            }
        });
        let objects = stream::iter(objects)
            .buffer_unordered(16) // TODO(low): is 16 a good number?
            .filter_map(|res| async move {
                match res {
                    Ok(object) => Some(Ok(object)),
                    Err(crate::Error::ObjectDoesNotExist(_)) if ignore_non_existing => None, // User lost read access to object between query and read
                    Err(err) => Some(Err(err)),
                }
            });
        pin_mut!(objects);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the objects to the client, batching them by messages of a reasonable size, to both allow for better
        // resumption after a connection loss, while not sending one message per mini-object.
        while let Some(object) = objects.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(low): is 1MiB a good number?
                let data = std::mem::replace(&mut current_data, Vec::new());
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
            let object = match object {
                Ok(object) => object,
                Err(err) => return Self::send_res(&mut conn.socket, request_id, Err(err)).await,
            };
            size_of_message += size_as_json(&object)?;
            current_data.push(object);
        }
        Self::send_res(
            &mut conn.socket,
            request_id,
            Ok(ResponsePart::Objects {
                data: current_data,
                now_have_all_until: None, // TODO(api): give real number here for Query answers
            }),
        )
        .await
    }

    async fn send_snapshots(
        &self,
        conn: &mut ConnectionState,
        request_id: RequestId,
        ignore_non_existing: bool,
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
                    // TODO(low): Think about whether to use postgres_db or cache_db for answering *Latest queries.
                    // Using cache_db means clobbering the cache with stuff useless for computing users_who_can_read,
                    // but can also mean faster QueryRemote(Importance::Latest) queries
                    let snapshot = self
                        .postgres_db
                        .get_latest_snapshot(user, object_id)
                        .await?;
                    Ok(MaybeSnapshot::NotSubscribed(snapshot))
                })
            }
        });
        let snapshots = stream::iter(snapshots)
            .buffer_unordered(16) // TODO(low): is 16 a good number?
            .filter_map(|res| async move {
                match res {
                    Ok(object) => Some(Ok(object)),
                    Err(crate::Error::ObjectDoesNotExist(_)) if ignore_non_existing => None, // User lost read access to object between query and read
                    Err(err) => Some(Err(err)),
                }
            });
        pin_mut!(snapshots);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the snapshots to the client, batching them by messages of a reasonable size, to both allow for better
        // resumption after a connection loss, while not sending one message per mini-object.
        while let Some(snapshot) = snapshots.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(low): is 1MiB a good number?
                let data = std::mem::replace(&mut current_data, Vec::new());
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
            let snapshot = match snapshot {
                Ok(snapshot) => snapshot,
                Err(err) => return Self::send_res(&mut conn.socket, request_id, Err(err)).await,
            };
            size_of_message += size_as_json(&snapshot)?;
            current_data.push(snapshot);
        }
        Self::send_res(
            &mut conn.socket,
            request_id,
            Ok(ResponsePart::Snapshots {
                data: current_data,
                now_have_all_until: None, // TODO(api): give real number here for Query answers
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
        let binaries = binaries.map(|binary_id| {
            self.cache_db
                .get_binary(binary_id)
                .map(move |r| (binary_id, r))
        });
        let binaries = stream::iter(binaries).buffer_unordered(16); // TODO(low): is 16 a good number?
        pin_mut!(binaries);
        let mut size_of_message = 0;
        let mut current_data = Vec::new();
        // Send all the binaries to the client, trying to avoid having too many ResponsePart::Binaries messages while still sending as
        // many binaries as possible before any potential error (in particular missing-binary).
        while let Some((binary_id, binary)) = binaries.next().await {
            if size_of_message >= 1024 * 1024 {
                // TODO(low): is 1MiB a good number?
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
    expected_binaries: usize,
    subscribed_objects: Arc<RwLock<HashSet<ObjectId>>>,
    subscribed_queries: Arc<RwLock<HashMap<QueryId, Arc<Query>>>>,
    updates_receiver: mpsc::UnboundedReceiver<Arc<(Updates, Option<serde_json::Value>, Vec<User>)>>,
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
