use super::{
    connection::{
        Command, Connection, ConnectionEvent, RequestWithSidecar, ResponsePartWithSidecar,
        ResponseSender,
    },
    LocalDb,
};
use crate::{
    api::{ApiConfig, UploadId},
    crdb_internal::Lock,
    db_trait::Db,
    error::ResultExt,
    future::{CrdbSend, CrdbSyncFn},
    ids::QueryId,
    messages::{
        MaybeObject, MaybeSnapshot, ObjectData, Request, ResponsePart, SnapshotData, Updates,
        Upload,
    },
    BinPtr, CrdbFuture, CrdbStream, Event, EventId, Object, ObjectId, Query, Session, SessionRef,
    SessionToken, TypeId, Updatedness,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::Either, pin_mut, stream, FutureExt, StreamExt};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    iter,
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::{oneshot, watch};

#[non_exhaustive]
pub enum OnError {
    Rollback,
    KeepLocal,
    ReplaceWith(Upload),
}

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    upload_queue_watcher_sender: Arc<Mutex<watch::Sender<Vec<UploadId>>>>,
    upload_queue_watcher_receiver: watch::Receiver<Vec<UploadId>>,
    upload_queue: Arc<LocalDb>,
    upload_resender: mpsc::UnboundedSender<(
        Option<UploadId>,
        Arc<Request>,
        mpsc::UnboundedSender<ResponsePartWithSidecar>,
    )>,
    connection_event_cb: Arc<RwLock<Box<dyn CrdbSyncFn<ConnectionEvent>>>>,
}

impl ApiDb {
    pub async fn new<C, GSO, GSQ, BG, EH, EHF, RRL>(
        upload_queue: Arc<LocalDb>,
        get_subscribed_objects: GSO,
        get_subscribed_queries: GSQ,
        binary_getter: Arc<BG>,
        error_handler: EH,
        require_relogin: RRL,
    ) -> crate::Result<(ApiDb, mpsc::UnboundedReceiver<Updates>)>
    where
        C: ApiConfig,
        GSO: 'static + CrdbSend + FnMut() -> HashMap<ObjectId, Option<Updatedness>>,
        GSQ: 'static
            + Send
            + FnMut() -> HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
        BG: 'static + Db,
        EH: 'static + CrdbSend + Fn(Upload, crate::Error) -> EHF,
        EHF: 'static + CrdbFuture<Output = OnError>,
        RRL: 'static + CrdbSend + Fn(),
    {
        let (update_sender, update_receiver) = mpsc::unbounded();
        let connection_event_cb: Arc<RwLock<Box<dyn CrdbSyncFn<ConnectionEvent>>>> =
            Arc::new(RwLock::new(Box::new(|_| ()) as _));
        let event_cb = {
            let connection_event_cb = connection_event_cb.clone();
            Box::new(move |evt| {
                let need_relogin = match evt {
                    ConnectionEvent::LoggingIn => false,
                    ConnectionEvent::FailedConnecting(_) => true,
                    ConnectionEvent::FailedSendingToken(_) => true,
                    ConnectionEvent::LostConnection(_) => false,
                    ConnectionEvent::InvalidToken(_) => true,
                    ConnectionEvent::Connected => false,
                    ConnectionEvent::TimeOffset(_) => false,
                    ConnectionEvent::LoggedOut => true,
                };
                if need_relogin {
                    (require_relogin)();
                }
                connection_event_cb.read().unwrap()(evt);
            })
        };
        let (connection, commands) = mpsc::unbounded();
        let (requests, requests_receiver) = mpsc::unbounded();
        crate::spawn(
            Connection::new(
                commands,
                requests_receiver,
                event_cb,
                update_sender,
                get_subscribed_objects,
                get_subscribed_queries,
            )
            .run(),
        );
        let all_uploads = upload_queue
            .list_uploads()
            .await
            .wrap_context("listing upload queue")?;
        let (upload_queue_watcher_sender, upload_queue_watcher_receiver) =
            watch::channel(all_uploads.clone());
        let upload_queue_watcher_sender = Arc::new(Mutex::new(upload_queue_watcher_sender));
        let (upload_resender_sender, upload_resender_receiver) = mpsc::unbounded();
        crate::spawn(upload_resender::<C, _, _, _>(
            upload_queue.clone(),
            upload_resender_receiver,
            requests,
            upload_queue_watcher_sender.clone(),
            binary_getter,
            error_handler,
        ));
        for upload_id in all_uploads {
            let upload = upload_queue
                .get_upload(upload_id)
                .await
                .wrap_context("retrieving upload")?
                .ok_or_else(|| {
                    crate::Error::Other(anyhow!(
                        "Upload vanished from queue while doing the initial read"
                    ))
                })?;
            let request = Arc::new(Request::Upload(upload));
            let (sender, _) = mpsc::unbounded(); // Ignore the response
            upload_resender_sender
                .unbounded_send((Some(upload_id), request, sender))
                .expect("connection cannot go away before apidb does");
        }
        Ok((
            ApiDb {
                upload_queue,
                upload_queue_watcher_sender,
                upload_queue_watcher_receiver,
                connection,
                upload_resender: upload_resender_sender,
                connection_event_cb,
            },
            update_receiver,
        ))
    }

    pub fn watch_upload_queue(&self) -> watch::Receiver<Vec<UploadId>> {
        self.upload_queue_watcher_receiver.clone()
    }

    pub fn on_connection_event(&self, cb: impl 'static + CrdbSyncFn<ConnectionEvent>) {
        *self.connection_event_cb.write().unwrap() = Box::new(cb);
    }

    pub fn login(&self, url: Arc<String>, token: SessionToken) {
        self.connection
            .unbounded_send(Command::Login { url, token })
            .expect("connection cannot go away before sender does")
    }

    pub fn logout(&self) {
        self.connection
            .unbounded_send(Command::Logout)
            .expect("connection cannot go away before sender does")
    }

    fn request(&self, request: Arc<Request>) -> mpsc::UnboundedReceiver<ResponsePartWithSidecar> {
        let (sender, response) = mpsc::unbounded();
        self.upload_resender
            .unbounded_send((None, request, sender))
            .expect("connection cannot go away before sender does");
        response
    }

    pub fn rename_session(&self, name: String) {
        self.request(Arc::new(Request::RenameSession(name)));
        // Ignore the response from the server
    }

    pub async fn current_session(&self) -> crate::Result<Session> {
        let response = self
            .request(Arc::new(Request::CurrentSession))
            .next()
            .await
            .ok_or_else(|| crate::Error::Other(anyhow!("Connection thread went down too early")))?;
        match response.response {
            ResponsePart::Sessions(mut sessions) if sessions.len() == 1 => {
                Ok(sessions.pop().unwrap())
            }
            ResponsePart::Error(err) => Err(err.into()),
            _ => Err(crate::Error::Other(anyhow!(
                "Unexpected server response to CurrentSession: {:?}",
                response.response
            ))),
        }
    }

    pub async fn list_sessions(&self) -> crate::Result<Vec<Session>> {
        let response = self
            .request(Arc::new(Request::ListSessions))
            .next()
            .await
            .ok_or_else(|| crate::Error::Other(anyhow!("Connection thread went down too early")))?;
        match response.response {
            ResponsePart::Sessions(sessions) => Ok(sessions),
            ResponsePart::Error(err) => Err(err.into()),
            _ => Err(crate::Error::Other(anyhow!(
                "Unexpected server response to ListSessions: {:?}",
                response.response
            ))),
        }
    }

    pub fn disconnect_session(
        &self,
        session_ref: SessionRef,
    ) -> oneshot::Receiver<crate::Result<()>> {
        let mut response_receiver = self.request(Arc::new(Request::DisconnectSession(session_ref)));
        let (sender, receiver) = oneshot::channel();
        crate::spawn(async move {
            let Some(response) = response_receiver.next().await else {
                let _ = sender.send(Err(crate::Error::Other(anyhow!(
                    "Connection thread went down too ealy"
                ))));
                return;
            };
            let _ = match response.response {
                ResponsePart::Success => sender.send(Ok(())),
                ResponsePart::Error(err) => sender.send(Err(err.into())),
                _ => sender.send(Err(crate::Error::Other(anyhow!(
                    "Unexpected server response to DisconnectSession: {:?}",
                    response.response
                )))),
            };
        });
        receiver
    }

    pub fn unsubscribe(&self, object_ids: HashSet<ObjectId>) {
        self.request(Arc::new(Request::Unsubscribe(object_ids)));
        // Ignore the response from the server, we don't care enough to wait for it
    }

    pub fn unsubscribe_query(&self, query_id: QueryId) {
        self.request(Arc::new(Request::UnsubscribeQuery(query_id)));
        // Ignore the response from the server, we don't care enough to wait for it
    }

    async fn handle_upload_response(
        mut receiver: mpsc::UnboundedReceiver<ResponsePartWithSidecar>,
    ) -> crate::Result<()> {
        match receiver.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection did not return any answer to query"
            ))),
            Some(ResponsePartWithSidecar {
                sidecar: Some(_), ..
            }) => Err(crate::Error::Other(anyhow!(
                "Connection returned sidecar while we expected a simple result"
            ))),
            Some(ResponsePartWithSidecar { response, .. }) => match response {
                ResponsePart::Success => Ok(()),
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Sessions(_)
                | ResponsePart::CurrentTime(_)
                | ResponsePart::Objects { .. }
                | ResponsePart::Snapshots { .. }
                | ResponsePart::Binaries(_) => Err(crate::Error::Other(anyhow!(
                    "Connection returned unexpected answer while expecting a simple result"
                ))),
            },
        }
    }

    pub async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        subscribe: bool,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let required_binaries = object.required_binaries();
        let upload = Upload::Object {
            object_id,
            type_id: *T::type_ulid(),
            created_at,
            snapshot_version: T::snapshot_version(),
            object: Arc::new(
                serde_json::to_value(object)
                    .wrap_context("serializing object for sending to api")?,
            ),
            subscribe,
        };
        let request = Arc::new(Request::Upload(upload.clone()));
        let (result_sender, result_receiver) = mpsc::unbounded();
        let upload_id = self
            .upload_queue
            .enqueue_upload(upload, required_binaries)
            .await
            .wrap_context("enqueuing upload")?;
        let upload_list = self
            .upload_queue
            .list_uploads()
            .await
            .wrap_context("listing uploads")?;
        self.upload_queue_watcher_sender
            .lock()
            .unwrap()
            .send_replace(upload_list);
        self.upload_resender
            .unbounded_send((Some(upload_id), request, result_sender))
            .map_err(|_| crate::Error::Other(anyhow!("Upload resender went out too early")))?;
        Ok(Self::handle_upload_response(result_receiver))
    }

    pub async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        subscribe: bool,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let required_binaries = event.required_binaries();
        let upload = Upload::Event {
            object_id,
            type_id: *T::type_ulid(),
            event_id,
            event: Arc::new(
                serde_json::to_value(event).wrap_context("serializing event for sending to api")?,
            ),
            subscribe,
        };
        let request = Arc::new(Request::Upload(upload.clone()));
        let (result_sender, result_receiver) = mpsc::unbounded();
        let upload_id = self
            .upload_queue
            .enqueue_upload(upload, required_binaries)
            .await
            .wrap_context("enqueuing upload")?;
        let upload_list = self
            .upload_queue
            .list_uploads()
            .await
            .wrap_context("listing uploads")?;
        self.upload_queue_watcher_sender
            .lock()
            .unwrap()
            .send_replace(upload_list);
        self.upload_resender
            .unbounded_send((Some(upload_id), request, result_sender))
            .map_err(|_| crate::Error::Other(anyhow!("Upload resender went out too early")))?;
        Ok(Self::handle_upload_response(result_receiver))
    }

    pub async fn get_subscribe(&self, object_id: ObjectId) -> crate::Result<ObjectData> {
        let mut object_ids = HashMap::new();
        object_ids.insert(object_id, None); // We do not know about this object yet, so None
        let request = Arc::new(Request::GetSubscribe(object_ids));
        let mut response = self.request(request);
        match response.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection-handling thread went out before ApiDb"
            ))),
            Some(response) => match response.response {
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Objects { mut data, .. } if data.len() == 1 => {
                    match data.pop().unwrap() {
                        MaybeObject::AlreadySubscribed(_) => Err(crate::Error::Other(anyhow!(
                            "Server unexpectedly told us we already know unknown {object_id:?}"
                        ))),
                        MaybeObject::NotYetSubscribed(res) => Ok(res),
                    }
                }
                _ => Err(crate::Error::Other(anyhow!(
                    "Unexpected response to GetSubscribe request: {:?}",
                    response.response
                ))),
            },
        }
    }

    pub async fn get_latest(&self, object_id: ObjectId) -> crate::Result<SnapshotData> {
        let mut object_ids = HashSet::new();
        object_ids.insert(object_id);
        let request = Arc::new(Request::GetLatest(object_ids));
        let mut response = self.request(request);
        match response.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection-handling thread went out before ApiDb"
            ))),
            Some(response) => match response.response {
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Snapshots { mut data, .. } if data.len() == 1 => {
                    match data.pop().unwrap() {
                        MaybeSnapshot::AlreadySubscribed(_) => Err(crate::Error::Other(anyhow!(
                            "Server unexpectedly told us we already know unknown {object_id:?}"
                        ))),
                        MaybeSnapshot::NotSubscribed(res) => Ok(res),
                    }
                }
                _ => Err(crate::Error::Other(anyhow!(
                    "Unexpected response to GetLatest request: {:?}",
                    response.response
                ))),
            },
        }
    }

    pub fn query_subscribe<T: Object>(
        &self,
        query_id: QueryId,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> impl CrdbStream<Item = crate::Result<(MaybeObject, Option<Updatedness>)>> {
        let request = Arc::new(Request::QuerySubscribe {
            query_id,
            type_id: *T::type_ulid(),
            query,
            only_updated_since,
        });
        self.request(request).flat_map(move |response| {
            match response.response {
                // No sidecar in answer to Request::Query
                ResponsePart::Error(err) => Either::Left(stream::iter(iter::once(Err(err.into())))),
                ResponsePart::Objects {
                    data,
                    now_have_all_until,
                } => {
                    let data_len = data.len();
                    Either::Right(stream::iter(data.into_iter().enumerate().map(
                        move |(i, d)| {
                            let now_have_all_until = if i + 1 == data_len {
                                now_have_all_until
                            } else {
                                None
                            };
                            Ok((d, now_have_all_until))
                        },
                    )))
                }
                resp => Either::Left(stream::iter(iter::once(Err(crate::Error::Other(anyhow!(
                    "Server gave unexpected answer to QuerySubscribe request: {resp:?}"
                )))))),
            }
        })
    }

    pub fn query_latest<T: Object>(
        &self,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> impl CrdbStream<Item = crate::Result<(MaybeSnapshot, Option<Updatedness>)>> {
        let request = Arc::new(Request::QueryLatest {
            type_id: *T::type_ulid(),
            query,
            only_updated_since,
        });
        self.request(request).flat_map(move |response| {
            match response.response {
                // No sidecar in answer to Request::Query
                ResponsePart::Error(err) => Either::Left(stream::iter(iter::once(Err(err.into())))),
                ResponsePart::Snapshots {
                    data,
                    now_have_all_until,
                } => {
                    let data_len = data.len();
                    Either::Right(stream::iter(data.into_iter().enumerate().map(
                        move |(i, d)| {
                            let now_have_all_until = if i + 1 == data_len {
                                now_have_all_until
                            } else {
                                None
                            };
                            Ok((d, now_have_all_until))
                        },
                    )))
                }
                resp => Either::Left(stream::iter(iter::once(Err(crate::Error::Other(anyhow!(
                    "Server gave unexpected answer to QueryLatest request: {resp:?}"
                )))))),
            }
        })
    }

    pub async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        let mut binary_ids = HashSet::new();
        binary_ids.insert(binary_id);
        let request = Arc::new(Request::GetBinaries(binary_ids));
        let mut response = self.request(request);
        match response.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection-handling thread went out before ApiDb"
            ))),
            Some(response) => match response.response {
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Binaries(1) => {
                    let bin = response.sidecar.ok_or_else(|| {
                        crate::Error::Other(anyhow!(
                            "Connection thread claimed to send us one binary but actually did not"
                        ))
                    })?;
                    Ok(Some(bin))
                }
                _ => Err(crate::Error::Other(anyhow!(
                    "Unexpected response to get-binary request: {:?}",
                    response.response
                ))),
            },
        }
    }
}

async fn upload_resender<C, BG, EH, EHF>(
    upload_queue: Arc<LocalDb>,
    requests: mpsc::UnboundedReceiver<(
        Option<UploadId>,
        Arc<Request>,
        mpsc::UnboundedSender<ResponsePartWithSidecar>,
    )>,
    connection: mpsc::UnboundedSender<(ResponseSender, Arc<RequestWithSidecar>)>,
    upload_queue_watcher_sender: Arc<Mutex<watch::Sender<Vec<UploadId>>>>,
    binary_getter: Arc<BG>,
    error_handler: EH,
) where
    C: ApiConfig,
    BG: Db,
    EH: 'static + CrdbSend + Fn(Upload, crate::Error) -> EHF,
    EHF: 'static + CrdbFuture<Output = OnError>,
{
    // The below loop is split into two sub parts: all that is just sent once, and all that requires
    // re-sending if there were missing binaries
    // This makes sure that all uploads have resolved before a query is submitted, while still allowing
    // uploads and queries to resolve in parallel.
    let requests = requests.peekable();
    pin_mut!(requests);
    macro_rules! poll_next_if {
        ($cond:expr) => {
            requests
                .as_mut()
                .peek()
                .now_or_never()
                .and_then(|req| req)
                .map($cond)
                .unwrap_or(false)
        };
    }
    while requests.as_mut().peek().await.is_some() {
        // First, handle all requests that require no re-sending. Just send them once and forget about them.
        while poll_next_if!(|(id, _, _)| id.is_none()) {
            let (upload_id, request, sender) = requests.next().await.unwrap();
            tracing::trace!(?request, "resender received non-upload request");
            assert!(upload_id.is_none(), "non-upload should not have an id");
            let _ = connection.unbounded_send((
                sender,
                Arc::new(RequestWithSidecar {
                    request,
                    sidecar: Vec::new(),
                }),
            ));
        }

        // Then, handle uploads. We start them all, and resend them with the missing binaries until we're successfully done.
        let mut upload_reqs = VecDeque::new();
        while poll_next_if!(|(id, _, _)| id.is_some()) {
            let (upload_id, request, final_sender) = requests.next().await.unwrap();
            let upload_id = upload_id.unwrap();
            tracing::trace!(?upload_id, ?request, "resender received upload request");
            let (sender, receiver) = mpsc::unbounded();
            upload_reqs.push_back((
                upload_id,
                Arc::new(RequestWithSidecar {
                    request,
                    sidecar: Vec::new(),
                }),
                Some(final_sender),
                sender,
                receiver,
            ));
        }
        let mut upload_missing_binaries = None;
        while !upload_reqs.is_empty() {
            // Before anything, attempt to upload the missing binaries if we're at least at the second loop turn
            // Ignore the result of uploading the binaries, as it's just a prerequisite for the other uploads here
            if let Some(upload_missing_binaries) = upload_missing_binaries.take() {
                let (sender, _) = mpsc::unbounded();
                let _ = connection.unbounded_send((sender, upload_missing_binaries));
            }

            // First, submit all requests
            for (_, request, _, sender, _) in upload_reqs.iter() {
                let _ = connection.unbounded_send((sender.clone(), request.clone()));
            }

            // Then, wait for them all to finish, listing the missing binaries
            // The successful or non-retryable requests get removed from upload_reqs here, by setting their final_sender to None
            let mut missing_binaries = HashSet::new();
            for (upload_id, request, final_sender, _, receiver) in upload_reqs.iter_mut() {
                match receiver.next().await {
                    None => return, // Connection was dropped
                    Some(ResponsePartWithSidecar {
                        sidecar: Some(_), ..
                    }) => {
                        tracing::error!("got response to upload that had a sidecar");
                        continue;
                    }
                    Some(ResponsePartWithSidecar { response, .. }) => match response {
                        ResponsePart::Success => {
                            if let Err(err) = upload_queue.upload_finished(*upload_id).await {
                                tracing::error!(?err, "failed dequeuing upload");
                            } else {
                                match upload_queue
                                    .list_uploads()
                                    .await
                                    .wrap_context("listing uploads")
                                {
                                    Err(err) => {
                                        tracing::error!(?err, "failed listing upload queue");
                                    }
                                    Ok(upload_list) => {
                                        upload_queue_watcher_sender
                                            .lock()
                                            .unwrap()
                                            .send_replace(upload_list);
                                    }
                                }
                                let _ = final_sender.take().unwrap().unbounded_send(
                                    ResponsePartWithSidecar {
                                        response,
                                        sidecar: None,
                                    },
                                );
                            }
                        }
                        ResponsePart::Error(crate::SerializableError::MissingBinaries(bins)) => {
                            missing_binaries.extend(bins);
                        }
                        ResponsePart::Error(crate::SerializableError::ObjectDoesNotExist(_))
                            if !missing_binaries.is_empty() =>
                        {
                            // Do nothing, and retry on the next round: this can happen if eg. object creation failed due to a missing binary
                            // If there was no missing binary yet, it means that there was no previous upload that we could retry.
                            // As such, in that situation, fall through to the next Error handling, and send the error back to the user.
                        }
                        ResponsePart::Error(ref err) => {
                            let Request::Upload(upload) = &*request.request else {
                                panic!("is_upload == true but does not match Upload");
                            };
                            match error_handler((*upload).clone(), (*err).clone().into()).await {
                                OnError::Rollback => {
                                    if let Err(err) = undo_upload::<C>(&upload_queue, upload).await
                                    {
                                        tracing::error!(?err, ?upload, "failed undoing upload");
                                    } else if let Err(err) =
                                        upload_queue.upload_finished(*upload_id).await
                                    {
                                        tracing::error!(?err, "failed dequeuing upload");
                                    } else {
                                        match upload_queue
                                            .list_uploads()
                                            .await
                                            .wrap_context("listing uploads")
                                        {
                                            Err(err) => {
                                                tracing::error!(
                                                    ?err,
                                                    "failed listing upload queue"
                                                );
                                            }
                                            Ok(upload_list) => {
                                                upload_queue_watcher_sender
                                                    .lock()
                                                    .unwrap()
                                                    .send_replace(upload_list);
                                            }
                                        }
                                        let _ = final_sender.take().unwrap().unbounded_send(
                                            ResponsePartWithSidecar {
                                                response,
                                                sidecar: None,
                                            },
                                        );
                                    }
                                }
                                OnError::KeepLocal => {
                                    // Do not remove the upload from the queue, so that it gets attempted again upon next
                                    // bootup. But do take the final_sender, so that we do not end up infinite-looping here.
                                    let _ = final_sender.take().unwrap().unbounded_send(
                                        ResponsePartWithSidecar {
                                            response,
                                            sidecar: None,
                                        },
                                    );
                                }
                                OnError::ReplaceWith(new_upload) => {
                                    if let Err(err) = undo_upload::<C>(&upload_queue, upload).await
                                    {
                                        tracing::error!(?err, ?upload, "failed undoing upload");
                                    } else if let Err(err) =
                                        do_upload::<C>(&upload_queue, &new_upload).await
                                    {
                                        tracing::error!(
                                            ?err,
                                            ?new_upload,
                                            "failed doing replacement upload"
                                        );
                                    } else if let Err(err) =
                                        upload_queue.upload_finished(*upload_id).await
                                    {
                                        tracing::error!(?err, "failed dequeuing upload");
                                    } else {
                                        match upload_queue
                                            .list_uploads()
                                            .await
                                            .wrap_context("listing uploads")
                                        {
                                            Err(err) => {
                                                tracing::error!(
                                                    ?err,
                                                    "failed listing upload queue"
                                                );
                                            }
                                            Ok(upload_list) => {
                                                upload_queue_watcher_sender
                                                    .lock()
                                                    .unwrap()
                                                    .send_replace(upload_list);
                                            }
                                        }
                                        let _ = final_sender.take().unwrap().unbounded_send(
                                            ResponsePartWithSidecar {
                                                response,
                                                sidecar: None,
                                            },
                                        );
                                    }
                                }
                            }
                        }
                        _ => {
                            tracing::error!(?response, "Unexpected response to upload submission");
                            continue;
                        }
                    },
                }
            }
            upload_reqs.retain(|(_, _, final_sender, _, _)| final_sender.is_some());

            // Were there missing binaries? If yes, prepend them to the list of requests to retry, and upload them this way.
            if !missing_binaries.is_empty() {
                let binary_getter = binary_getter.clone();
                let binaries = stream::iter(missing_binaries.into_iter())
                    .map(move |b| {
                        let binary_getter = binary_getter.clone();
                        async move { binary_getter.get_binary(b).await }
                    })
                    .buffer_unordered(16) // TODO(perf-low): is 16 a good number?
                    .filter_map(|res| async move { res.ok().and_then(|o| o) })
                    .collect::<Vec<Arc<[u8]>>>()
                    .await;
                upload_missing_binaries = Some(Arc::new(RequestWithSidecar {
                    request: Arc::new(Request::UploadBinaries(binaries.len())),
                    sidecar: binaries,
                }));
            }
        }
    }
}

async fn undo_upload<C: ApiConfig>(local_db: &LocalDb, upload: &Upload) -> crate::Result<()> {
    match upload {
        Upload::Object { object_id, .. } => local_db.remove(*object_id).await,
        Upload::Event {
            object_id,
            type_id,
            event_id,
            ..
        } => match C::remove_event(local_db, *type_id, *object_id, *event_id).await {
            Err(crate::Error::EventTooEarly { .. }) => {
                // EventTooEarly means that the object has been recreated since the event was submitted
                // In turn, this means that the server has pushed a re-creation update that was accepted
                // As such, the event was already undone, by application of the update
                Ok(())
            }
            res => res,
        },
    }
}

async fn do_upload<C: ApiConfig>(local_db: &LocalDb, upload: &Upload) -> crate::Result<()> {
    match upload {
        Upload::Object {
            object_id,
            type_id,
            created_at,
            snapshot_version,
            object,
            ..
        } => {
            C::create(
                local_db,
                *type_id,
                *object_id,
                *created_at,
                *snapshot_version,
                object,
            )
            .await
        }
        Upload::Event {
            object_id,
            type_id,
            event_id,
            event,
            ..
        } => C::submit(
            local_db,
            *type_id,
            *object_id,
            *event_id,
            event,
            None,
            Lock::NONE,
        )
        .await
        .map(|_| ()),
    }
}
