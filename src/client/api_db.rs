use super::Authenticator;
use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CanDoCallbacks, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
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
}

#[allow(unused_variables)] // TODO: remove
impl<A: Authenticator> Db for ApiDb<A> {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        _cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
