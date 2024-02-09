use super::{
    connection::{
        Command, Connection, ConnectionEvent, RequestWithSidecar, ResponsePartWithSidecar,
        ResponseSender,
    },
    ShouldLock,
};
use crate::{
    db_trait::Db,
    error::ResultExt,
    ids::QueryId,
    messages::{MaybeObject, ObjectData, Request, RequestKind, ResponsePart, Updates, Upload},
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, TypeId, Updatedness,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::Either, pin_mut, stream, StreamExt};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    iter,
    sync::{Arc, RwLock},
};

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    upload_resender:
        mpsc::UnboundedSender<(Arc<Request>, mpsc::UnboundedSender<ResponsePartWithSidecar>)>,
    connection_event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
}

impl ApiDb {
    pub fn new<GSO, GSQ, D>(
        get_subscribed_objects: GSO,
        get_subscribed_queries: GSQ,
        binary_getter: Arc<D>,
        error_sender: mpsc::UnboundedSender<crate::Error>,
    ) -> (ApiDb, mpsc::UnboundedReceiver<Updates>)
    where
        GSO: 'static + Send + FnMut() -> HashMap<ObjectId, Option<Updatedness>>,
        GSQ: 'static
            + Send
            + FnMut() -> HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, ShouldLock)>,
        D: 'static + Db,
    {
        let (update_sender, update_receiver) = mpsc::unbounded();
        let connection_event_cb = Arc::new(RwLock::new(Box::new(|_| ()) as _));
        let (connection, commands) = mpsc::unbounded();
        let (requests, requests_receiver) = mpsc::unbounded();
        crate::spawn(
            Connection::new(
                commands,
                requests_receiver,
                connection_event_cb.clone(),
                update_sender,
                get_subscribed_objects,
                get_subscribed_queries,
            )
            .run(),
        );
        let (upload_resender, upload_resender_receiver) = mpsc::unbounded();
        crate::spawn(Self::upload_resender(
            upload_resender_receiver,
            requests,
            binary_getter,
            error_sender,
        ));
        (
            ApiDb {
                connection,
                upload_resender,
                connection_event_cb,
            },
            update_receiver,
        )
    }

    pub fn on_connection_event(&self, cb: impl 'static + Send + Sync + Fn(ConnectionEvent)) {
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
            .unbounded_send((request, sender))
            .expect("connection cannot go away before sender does");
        response
    }

    pub fn unsubscribe(&self, object_ids: HashSet<ObjectId>) {
        self.request(Arc::new(Request::Unsubscribe(object_ids)));
        // Ignore the response from the server, we don't care enough to wait for it
    }

    pub fn unsubscribe_query(&self, query_id: QueryId) {
        self.request(Arc::new(Request::UnsubscribeQuery(query_id)));
        // Ignore the response from the server, we don't care enough to wait for it
    }

    async fn upload_resender<D: Db>(
        requests: mpsc::UnboundedReceiver<(
            Arc<Request>,
            mpsc::UnboundedSender<ResponsePartWithSidecar>,
        )>,
        connection: mpsc::UnboundedSender<(ResponseSender, Arc<RequestWithSidecar>)>,
        binary_getter: Arc<D>,
        _error_sender: mpsc::UnboundedSender<crate::Error>,
    ) {
        // The below loop is split into three sub parts: session-related requests, upload submission, and query submission.
        // This makes sure that all uploads have resolved before a query is submitted, while still allowing
        // uploads and queries to resolve in parallel.
        let requests = requests.peekable();
        pin_mut!(requests);
        while requests.as_mut().peek().await.is_some() {
            // First, handle all session requests. These ones, we just send and don't care about the result.
            while requests
                .as_mut()
                .peek()
                .await
                .map(|(r, _)| r.kind() == RequestKind::Session)
                .unwrap_or(false)
            {
                let (request, sender) = requests.next().await.unwrap();
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
            while requests
                .as_mut()
                .peek()
                .await
                .map(|(r, _)| r.kind() == RequestKind::Upload)
                .unwrap_or(false)
            {
                let (request, final_sender) = requests.next().await.unwrap();
                let (sender, receiver) = mpsc::unbounded();
                upload_reqs.push_back((
                    Arc::new(RequestWithSidecar {
                        request,
                        sidecar: Vec::new(),
                    }),
                    Some(final_sender),
                    sender,
                    receiver,
                ));
            }
            while !upload_reqs.is_empty() {
                // First, submit all requests
                for (request, _, sender, _) in upload_reqs.iter() {
                    let _ = connection.unbounded_send((sender.clone(), request.clone()));
                }

                // Then, wait for them all to finish, listing the missing binaries
                // The successful or non-retryable requests get removed from upload_reqs here, by setting their final_sender to None
                let mut missing_binaries = HashSet::new();
                for (_, final_sender, _, receiver) in upload_reqs.iter_mut() {
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
                                let _ = final_sender.take().unwrap().unbounded_send(
                                    ResponsePartWithSidecar {
                                        response,
                                        sidecar: None,
                                    },
                                );
                            }
                            ResponsePart::Error(crate::SerializableError::MissingBinaries(
                                bins,
                            )) => {
                                missing_binaries.extend(bins);
                            }
                            ResponsePart::Error(crate::SerializableError::ObjectDoesNotExist(
                                _,
                            )) if !missing_binaries.is_empty() => {
                                // Do nothing, and retry on the next round: this can happen if eg. object creation failed due to a missing binary
                                // If there was no missing binary yet, it means that there was no previous upload that we could retry.
                                // As such, in that situation, fall through to the next Error handling, and send the error back to the user.
                            }
                            ResponsePart::Error(_) => {
                                let _ = final_sender.take().unwrap().unbounded_send(
                                    ResponsePartWithSidecar {
                                        response,
                                        sidecar: None,
                                    },
                                );
                            }
                            _ => {
                                tracing::error!(
                                    ?response,
                                    "Unexpected response to upload submission"
                                );
                                continue;
                            }
                        },
                    }
                }
                upload_reqs.retain(|(_, final_sender, _, _)| final_sender.is_some());

                // Were there missing binaries? If yes, prepend them to the list of requests to retry, and upload them this way.
                if !missing_binaries.is_empty() {
                    let (sender, receiver) = mpsc::unbounded();
                    let (final_sender, _) = mpsc::unbounded();
                    let binary_getter = binary_getter.clone();
                    let binaries = stream::iter(missing_binaries.into_iter())
                        .map(move |b| {
                            let binary_getter = binary_getter.clone();
                            async move { binary_getter.get_binary(b).await }
                        })
                        .buffer_unordered(16) // TODO(low): is 16 a good number?
                        .filter_map(|res| async move { res.ok().and_then(|o| o) })
                        .collect::<Vec<Arc<[u8]>>>()
                        .await;
                    let request = Arc::new(RequestWithSidecar {
                        request: Arc::new(Request::UploadBinaries(binaries.len())),
                        sidecar: binaries,
                    });
                    upload_reqs.push_front((request, Some(final_sender), sender, receiver));
                }
            }

            // Finally, handle queries
            // TODO(api)
        }
    }

    #[allow(dead_code)] // TODO(api): remove once the above is implemented
    async fn auto_resender_with_binaries<D: Db>(
        request: Arc<Request>,
        requests: mpsc::UnboundedSender<(ResponseSender, Arc<RequestWithSidecar>)>,
        binary_getter: Arc<D>,
    ) -> crate::Result<()> {
        let request = Arc::new(RequestWithSidecar {
            request,
            sidecar: Vec::new(),
        });
        loop {
            // Send the request
            let (response_sender, mut response_receiver) = mpsc::unbounded();
            if requests
                .unbounded_send((response_sender, request.clone()))
                .is_err()
            {
                return Err(crate::Error::Other(anyhow!(
                    "Connection-handling thread shut down"
                )));
            }

            // Wait for a response
            let Some(response) = response_receiver.next().await else {
                // No response, connection thread shut down.
                return Err(crate::Error::Other(anyhow!(
                    "Connection-handling thread never returned a response for request"
                )));
            };
            let response = response.response; // No sidecar in creation responses

            // Handle the response
            match response {
                ResponsePart::Success => return Ok(()),
                ResponsePart::Sessions(_)
                | ResponsePart::CurrentTime(_)
                | ResponsePart::Objects { .. }
                | ResponsePart::Binaries(_)
                | ResponsePart::Snapshots { .. } => {
                    return Err(crate::Error::Other(anyhow!(
                        "Server broke protocol by answering a creation request with {response:?}"
                    )));
                }
                ResponsePart::Error(crate::SerializableError::MissingBinaries(
                    missing_binaries,
                )) => {
                    // TODO(low): Currently, if the user enqueues 3 objects that all require the same binary at
                    // once, then the server will reject all 3 of them and then the client will upload the binary
                    // 3 times before noticing that it's now all good. Maybe some more global knowledge of what's
                    // being sent would allow to avoid that case?

                    // Send all missing binaries, ignoring the server's answer to the binaries, then re-enqueue the request
                    for binary_id in missing_binaries {
                        let bin = binary_getter
                            .get_binary(binary_id)
                            .await?
                            .ok_or_else(|| crate::Error::MissingBinaries(vec![binary_id]))?;
                        let bin_request = Arc::new(RequestWithSidecar {
                            request: Arc::new(Request::UploadBinaries(1)),
                            sidecar: vec![bin],
                        });
                        let (response_sender, _) = mpsc::unbounded();
                        if requests
                            .unbounded_send((response_sender, bin_request))
                            .is_err()
                        {
                            return Err(crate::Error::Other(anyhow!(
                                "Connection-handling thread shut down"
                            )));
                        }
                    }
                    continue; // re-enqueue the request on next loop turn
                }
                ResponsePart::Error(crate::SerializableError::ObjectDoesNotExist(_object_id)) => {
                    // We can hit this place if:
                    // - an object is submitted with missing-on-server binaries
                    // - an event is submitted
                    // - the server replies with missing-binaries, re-enqueuing the object
                    // - the event then would get an answer of object-does-not-exist
                    // As such, it is enough to just re-enqueue the event and it will eventually resolve, once the
                    // object will have been processed. DO NOT enqueue a new request for object creation, that would
                    // then put the same object twice in the queue. Not a big deal, but pointless effort.
                    continue; // try again
                }
                ResponsePart::Error(crate::SerializableError::ConnectionLoss) => continue, // try again
                ResponsePart::Error(err) => return Err(err.into()),
            }
        }
    }

    async fn expect_one_result(
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

    pub fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        subscribe: bool,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let request = Arc::new(Request::Upload(Upload::Object {
            object_id,
            type_id: *T::type_ulid(),
            created_at,
            snapshot_version: T::snapshot_version(),
            object: serde_json::to_value(object)
                .wrap_context("serializing object for sending to api")?,
            subscribe,
        }));
        let (result_sender, result_receiver) = mpsc::unbounded();
        self.upload_resender
            .unbounded_send((request, result_sender))
            .map_err(|_| crate::Error::Other(anyhow!("Upload resender went out too early")))?;
        Ok(Self::expect_one_result(result_receiver))
    }

    pub fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        subscribe: bool,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let request = Arc::new(Request::Upload(Upload::Event {
            object_id,
            type_id: *T::type_ulid(),
            event_id,
            event: Arc::new(
                serde_json::to_value(event).wrap_context("serializing event for sending to api")?,
            ),
            subscribe,
        }));
        let (result_sender, result_receiver) = mpsc::unbounded();
        self.upload_resender
            .unbounded_send((request, result_sender))
            .map_err(|_| crate::Error::Other(anyhow!("Upload resender went out too early")))?;
        Ok(Self::expect_one_result(result_receiver))
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
                    "Unexpected response to get request: {:?}",
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
                    "Server gave unexpected answer to Query request: {resp:?}"
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
