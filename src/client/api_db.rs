use super::connection::{Command, Connection, ConnectionEvent};
use crate::{
    db_trait::Db,
    error::ResultExt,
    messages::{ObjectData, Request, ResponsePart, Update, Upload, UploadOrBinary},
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp,
};
use anyhow::anyhow;
use futures::{channel::mpsc, StreamExt};
use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, RwLock},
};
use tokio::sync::oneshot;

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    requests: mpsc::UnboundedSender<(mpsc::UnboundedSender<ResponsePart>, Arc<Request>)>,
    connection_event_cb: Arc<RwLock<Box<dyn Send + Sync + Fn(ConnectionEvent)>>>,
}

impl ApiDb {
    pub fn new() -> (ApiDb, mpsc::UnboundedReceiver<Update>) {
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

    fn request(&self, request: Arc<Request>) -> mpsc::UnboundedReceiver<ResponsePart> {
        let (sender, response) = mpsc::unbounded();
        self.requests
            .unbounded_send((sender, request))
            .expect("connection cannot go away before sender does");
        response
    }

    pub fn unsubscribe(&self, object_ids: HashSet<ObjectId>) {
        self.request(Arc::new(Request::Unsubscribe(object_ids)));
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
        requests: mpsc::UnboundedSender<(mpsc::UnboundedSender<ResponsePart>, Arc<Request>)>,
        _binary_getter: Arc<D>,
    ) -> crate::Result<()> {
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

            // Handle the response
            match response {
                ResponsePart::ConnectionLoss => continue, // try again
                ResponsePart::Success => return Ok(()),
                ResponsePart::Sessions(_)
                | ResponsePart::CurrentTime(_)
                | ResponsePart::Objects { .. } => {
                    return Err(crate::Error::Other(anyhow!(
                        "Server broke protocol by answering a creation request with {response:?}"
                    )));
                }
                ResponsePart::Error(crate::SerializableError::MissingBinaries(
                    _missing_binaries,
                )) => {
                    unimplemented!() // TODO(api)
                }
                ResponsePart::Error(err) => return Err(err.into()),
            }
        }
    }

    pub fn create<T: Object, D: Db>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
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
        _object: ObjectId,
        _event_id: EventId,
        _event: Arc<T::Event>,
        _binary_getter: Arc<D>,
        _error_sender: mpsc::UnboundedSender<crate::Error>,
    ) -> oneshot::Receiver<crate::Result<()>> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get_all(&self, _object_id: ObjectId) -> crate::Result<ObjectData> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn query<T: Object>(
        &self,
        _only_updated_since: Option<Timestamp>,
        _q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<ObjectData>>> {
        // unimplemented!() // TODO(api): implement
        Ok(futures::stream::empty())
    }

    #[allow(dead_code)] // TODO(api): this will be required by the logic for object resubmission
    pub async fn create_binary(&self, _binary_id: BinPtr, _data: Arc<[u8]>) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
        unimplemented!() // TODO(api): implement
    }
}
