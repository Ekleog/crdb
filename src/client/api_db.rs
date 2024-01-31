use super::connection::{Command, Connection, ConnectionEvent, ResponseSender};
use crate::{
    db_trait::Db,
    error::ResultExt,
    ids::QueryId,
    messages::{
        MaybeObject, ObjectData, Request, RequestWithSidecar, ResponsePart,
        ResponsePartWithSidecar, Updates, Upload, UploadOrBinary,
    },
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Updatedness,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::Either, stream, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    iter,
    sync::{Arc, RwLock},
};
use tokio::sync::oneshot;

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    requests: mpsc::UnboundedSender<(ResponseSender, Arc<RequestWithSidecar>)>,
    connection_event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
}

impl ApiDb {
    pub fn new() -> (ApiDb, mpsc::UnboundedReceiver<Updates>) {
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
            )
            .run(),
        );
        (
            ApiDb {
                connection,
                requests,
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

    fn request(
        &self,
        request: Arc<RequestWithSidecar>,
    ) -> mpsc::UnboundedReceiver<ResponsePartWithSidecar> {
        let (sender, response) = mpsc::unbounded();
        self.requests
            .unbounded_send((sender, request))
            .expect("connection cannot go away before sender does");
        response
    }

    pub fn unsubscribe(&self, object_ids: HashSet<ObjectId>) {
        self.request(Arc::new(RequestWithSidecar {
            request: Arc::new(Request::Unsubscribe(object_ids)),
            sidecar: Vec::new(),
        }));
        // Ignore the response from the server, we don't care enough to wait for it
    }

    async fn error_catcher(
        future: impl Future<Output = crate::Result<()>>,
        result_sender: oneshot::Sender<crate::Result<()>>,
        error_sender: mpsc::UnboundedSender<crate::Error>,
    ) {
        let res = future.await;
        if let Err(res) = result_sender.send(res) {
            // Failed sending to the result sender, send to error sender if required
            if let Err(err) = res {
                // If no one is listening on the error receiver, then too bad the user will never know of the error
                let _ = error_sender.unbounded_send(err);
            }
        }
    }

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
                | ResponsePart::Snapshots(_) => {
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
                            request: Arc::new(Request::Upload(vec![UploadOrBinary::Binary])),
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

    pub fn create<T: Object, D: Db>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        subscribe: bool,
        binary_getter: Arc<D>,
        error_sender: mpsc::UnboundedSender<crate::Error>,
    ) -> crate::Result<oneshot::Receiver<crate::Result<()>>> {
        let request = Arc::new(Request::Upload(vec![UploadOrBinary::Upload(
            Upload::Object {
                object_id,
                type_id: *T::type_ulid(),
                created_at,
                snapshot_version: T::snapshot_version(),
                object: serde_json::to_value(object)
                    .wrap_context("serializing object for sending to api")?,
                subscribe,
            },
        )]));
        let (result_sender, result_receiver) = oneshot::channel();
        crate::spawn(Self::error_catcher(
            Self::auto_resender_with_binaries(request, self.requests.clone(), binary_getter),
            result_sender,
            error_sender,
        ));
        Ok(result_receiver)
    }

    pub fn submit<T: Object, D: Db>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        subscribe: bool,
        binary_getter: Arc<D>,
        error_sender: mpsc::UnboundedSender<crate::Error>,
    ) -> crate::Result<oneshot::Receiver<crate::Result<()>>> {
        let request = Arc::new(Request::Upload(vec![UploadOrBinary::Upload(
            Upload::Event {
                object_id,
                type_id: *T::type_ulid(),
                event_id,
                event: serde_json::to_value(event)
                    .wrap_context("serializing event for sending to api")?,
                subscribe,
            },
        )]));
        let (result_sender, result_receiver) = oneshot::channel();
        crate::spawn(Self::error_catcher(
            Self::auto_resender_with_binaries(request, self.requests.clone(), binary_getter),
            result_sender,
            error_sender,
        ));
        Ok(result_receiver)
    }

    // TODO(low): Think about whether to use postgres_db or cache_db for answering *Latest queries.
    // Using cache_db means clobbering the cache with stuff useless for computing users_who_can_read,
    // but can also mean faster QueryRemote(Importance::Latest) queries

    pub async fn get_subscribe(&self, object_id: ObjectId) -> crate::Result<ObjectData> {
        let mut object_ids = HashMap::new();
        object_ids.insert(object_id, None); // We do not know about this object yet, so None
        let request = Arc::new(Request::GetSubscribe(object_ids));
        let mut response = self.request(Arc::new(RequestWithSidecar {
            request,
            sidecar: Vec::new(),
        }));
        match response.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection-handling thread went out before ApiDb"
            ))),
            Some(response) => match response.response {
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Objects { mut data, .. } if data.len() == 1 => {
                    // TODO(client): record now_have_all_until somewhere (for queries)
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
    ) -> impl CrdbStream<Item = crate::Result<MaybeObject>> {
        let request = Arc::new(RequestWithSidecar {
            request: Arc::new(Request::QuerySubscribe {
                query_id,
                type_id: *T::type_ulid(),
                query,
                only_updated_since,
            }),
            sidecar: Vec::new(),
        });
        self.request(request).flat_map(|response| {
            match response.response {
                // No sidecar in answer to Request::Query
                ResponsePart::Error(err) => Either::Left(stream::iter(iter::once(Err(err.into())))),
                ResponsePart::Objects {
                    data,
                    now_have_all_until: _, // TODO(client): record now_have_all_until somewhere (for queries)
                } => Either::Right(stream::iter(data.into_iter().map(Ok))),
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
        let mut response = self.request(Arc::new(RequestWithSidecar {
            request,
            sidecar: Vec::new(),
        }));
        match response.next().await {
            None => Err(crate::Error::Other(anyhow!(
                "Connection-handling thread went out before ApiDb"
            ))),
            Some(mut response) => match response.response {
                ResponsePart::Error(err) => Err(err.into()),
                ResponsePart::Binaries(1) => {
                    if response.sidecar.len() != 1 {
                        Err(crate::Error::Other(anyhow!(
                            "Server claimed to send us one binary but actually sent {}",
                            response.sidecar.len()
                        )))
                    } else {
                        Ok(Some(response.sidecar.pop().unwrap()))
                    }
                }
                _ => Err(crate::Error::Other(anyhow!(
                    "Unexpected response to get-binary request: {:?}",
                    response.response
                ))),
            },
        }
    }
}
