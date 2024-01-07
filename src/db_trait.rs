use crate::{
    api::{BinPtr, Query},
    full_object::{DynSized, FullObject},
    CanDoCallbacks, EventId, Object, ObjectId, TypeId, User,
};
use futures::Stream;
use std::{future::Future, sync::Arc};
use ulid::Ulid;

#[derive(Clone)]
pub struct DynNewObject {
    pub type_id: TypeId,
    pub id: ObjectId,
    pub created_at: EventId,
    pub object: Arc<dyn DynSized>,
}

#[derive(Clone)]
pub struct DynNewEvent {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub id: EventId,
    pub event: Arc<dyn DynSized>,
}

#[derive(Clone, Debug)]
pub struct DynNewRecreation {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub time: Timestamp,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[cfg_attr(test, derive(bolero::generator::TypeGenerator))]
pub struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

impl Timestamp {
    pub fn from_ms(v: u64) -> Timestamp {
        Timestamp(v)
    }

    pub fn max_for_ulid() -> Timestamp {
        Timestamp((1 << Ulid::TIME_BITS) - 1)
    }

    pub fn time_ms(&self) -> u64 {
        self.0
    }
}

pub trait Db: 'static + Send + Sync {
    /// These streams get new elements whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn new_objects(&self) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewObject>>;
    /// This function returns all new events for events on objects that have been subscribed
    /// on. Objects subscribed on are all the objects that have ever been created
    /// with `created`, or obtained with `get` or `query`, as well as all objects
    /// received through `new_objects`, excluding objects explicitly unsubscribed from
    fn new_events(&self) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewEvent>>;
    fn new_recreations(
        &self,
    ) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewRecreation>>;
    /// Note that this function unsubscribes ALL the streams that have ever been taken from this
    /// database; and purges it from the local database.
    fn unsubscribe(&self, ptr: ObjectId) -> impl Send + Future<Output = anyhow::Result<()>>;

    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> impl Send + Future<Output = crate::Result<()>>;
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl Send + Future<Output = crate::Result<()>>;

    fn get<T: Object>(
        &self,
        ptr: ObjectId,
    ) -> impl Send + Future<Output = crate::Result<FullObject>>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> impl Send + Future<Output = anyhow::Result<impl Stream<Item = crate::Result<FullObject>>>>;

    fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> impl Send + Future<Output = crate::Result<()>>;

    fn create_binary(
        &self,
        id: BinPtr,
        value: Arc<Vec<u8>>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn get_binary(
        &self,
        ptr: BinPtr,
    ) -> impl Send + Future<Output = anyhow::Result<Option<Arc<Vec<u8>>>>>;
}
