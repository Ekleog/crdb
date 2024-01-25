use super::connection::{Command, Connection, ConnectionEvent};
use crate::{
    messages::{ObjectData, Request, ResponsePart, Update},
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp,
};
use anyhow::anyhow;
use futures::{channel::mpsc, StreamExt};
use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

pub struct ApiDb {
    connection: mpsc::UnboundedSender<Command>,
    requests: mpsc::UnboundedSender<(mpsc::UnboundedSender<ResponsePart>, Request)>,
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

    async fn request(&self, request: Request) -> mpsc::UnboundedReceiver<ResponsePart> {
        let (sender, response) = mpsc::unbounded();
        self.requests
            .unbounded_send((sender, request))
            .expect("connection cannot go away before sender does");
        response
    }

    pub async fn unsubscribe(&self, object_ids: HashSet<ObjectId>) -> crate::Result<()> {
        let mut response = self.request(Request::Unsubscribe(object_ids)).await;
        match response.next().await {
            None => Ok(()), // Lost connection, we will be unsubscribed anyway
            Some(ResponsePart::Success) => Ok(()),
            Some(ResponsePart::Error(err)) => Err(err.into()),
            Some(resp) => Err(crate::Error::Other(anyhow!(
                "Unexpected server response to Unsubscribe request: {resp:?}"
            ))),
        }
    }

    pub async fn create<T: Object>(
        &self,
        _id: ObjectId,
        _created_at: EventId,
        _object: Arc<T>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn submit<T: Object>(
        &self,
        _object: ObjectId,
        _event_id: EventId,
        _event: Arc<T::Event>,
    ) -> crate::Result<()> {
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

    pub async fn create_binary(&self, _binary_id: BinPtr, _data: Arc<[u8]>) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
        unimplemented!() // TODO(api): implement
    }
}
