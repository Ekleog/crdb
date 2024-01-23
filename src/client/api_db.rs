use crate::{
    db_trait::{DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp,
};
use std::sync::{Arc, RwLock};

pub enum ConnectionState {
    Connected,
    Disconnected,
    InvalidToken,
}

enum State {
    NotLoggedInYet,
    InvalidToken,
    Disconnected {
        token: SessionToken,
    },
    Connected {
        token: SessionToken,
        // TODO(api): keep running websocket feed
    },
}

pub struct ApiDb {
    base_url: Arc<String>,
    state: State,
    connection_state_change_cb: RwLock<Box<dyn Fn(ConnectionState)>>,
}

impl ApiDb {
    pub fn new(base_url: Arc<String>) -> ApiDb {
        ApiDb {
            base_url,
            state: State::NotLoggedInYet,
            connection_state_change_cb: RwLock::new(Box::new(|_| ())),
        }
    }

    pub fn on_connection_state_change(&self, cb: impl 'static + Fn(ConnectionState)) {
        *self.connection_state_change_cb.write().unwrap() = Box::new(cb);
    }

    pub fn login(&self, _token: SessionToken) -> anyhow::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn logout(&self) -> anyhow::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    // TODO(api): use the async_broadcast crate with overflow disabled to fan-out in a blocking manner the new_object/event notifications
    pub async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        // unimplemented!() // TODO(api): implement
        futures::stream::empty()
    }

    /// This function returns all new events for events on objects that have been subscribed
    /// on. Objects subscribed on are all the objects that have ever been created with `create`,
    /// or obtained with `get` or `query` and subscribed on, excluding objects explicitly
    /// unsubscribed from
    pub async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        // unimplemented!() // TODO(api): implement
        futures::stream::empty()
    }

    pub async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        // unimplemented!() // TODO(api): implement
        futures::stream::empty()
    }

    /// Note that this function unsubscribes ALL the streams that have ever been taken for
    /// this object; and purges it from the local database.
    pub async fn unsubscribe(&self, _ptr: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
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

    pub async fn get<T: Object>(&self, _object_id: ObjectId) -> crate::Result<FullObject> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn query<T: Object>(
        &self,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // unimplemented!() // TODO(api): implement
        Ok(futures::stream::empty())
    }

    pub async fn recreate<T: Object>(
        &self,
        _time: Timestamp,
        _object: ObjectId,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn create_binary(
        &self,
        _binary_id: BinPtr,
        _data: Arc<Vec<u8>>,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(api): implement
    }

    pub async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        unimplemented!() // TODO(api): implement
    }
}
