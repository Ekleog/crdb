use super::Authenticator;
use crate::{
    db_trait::{DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use std::sync::Arc;

pub struct ApiDb<A: Authenticator> {
    _auth: A,
    user: User,
}

impl<A: Authenticator> ApiDb<A> {
    pub async fn connect(_base_url: Arc<String>, _auth: Arc<A>) -> anyhow::Result<ApiDb<A>> {
        todo!()
    }

    pub fn user(&self) -> User {
        self.user
    }

    pub async fn disconnect(&self) -> anyhow::Result<()> {
        todo!()
    }

    // TODO: use the async_broadcast crate with overflow disabled to fan-out in a blocking manner the new_object/event notifications
    pub async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        // todo!()
        futures::stream::empty()
    }

    /// This function returns all new events for events on objects that have been subscribed
    /// on. Objects subscribed on are all the objects that have ever been created
    /// with `created`, or obtained with `get` or `query`, as well as all objects
    /// received through `new_objects`, excluding objects explicitly unsubscribed from
    pub async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        // todo!()
        futures::stream::empty()
    }

    pub async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        // todo!()
        futures::stream::empty()
    }

    /// Note that this function unsubscribes ALL the streams that have ever been taken for
    /// this object; and purges it from the local database.
    pub async fn unsubscribe(&self, _ptr: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn create<T: Object>(
        &self,
        _id: ObjectId,
        _created_at: EventId,
        _object: Arc<T>,
    ) -> crate::Result<()> {
        todo!()
    }

    pub async fn submit<T: Object>(
        &self,
        _object: ObjectId,
        _event_id: EventId,
        _event: Arc<T::Event>,
    ) -> crate::Result<()> {
        todo!()
    }

    pub async fn get<T: Object>(&self, _object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
    }

    pub async fn query<T: Object>(
        &self,
        _user: User,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: &Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    pub async fn recreate<T: Object>(
        &self,
        _time: Timestamp,
        _object: ObjectId,
    ) -> crate::Result<()> {
        todo!()
    }

    pub async fn create_binary(
        &self,
        _binary_id: BinPtr,
        _data: Arc<Vec<u8>>,
    ) -> crate::Result<()> {
        todo!()
    }

    pub async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
