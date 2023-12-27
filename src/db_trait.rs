use crate::{
    api::{BinPtr, Query},
    full_object::{DynSized, FullObject},
    Object, User,
};
use anyhow::anyhow;
use futures::Stream;
use std::{future::Future, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectId(pub Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId(pub Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TypeId(pub Ulid);

macro_rules! impl_for_id {
    ($type:ty) => {
        impl $type {
            #[allow(dead_code)]
            pub(crate) fn time(&self) -> Timestamp {
                Timestamp(self.0.timestamp_ms())
            }
        }
        deepsize::known_deep_size!(0; $type); // These types does not allocate
    };
}

impl_for_id!(ObjectId);
impl_for_id!(EventId);
impl_for_id!(TypeId);

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

#[derive(Clone)]
pub struct DynNewSnapshot {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub time: Timestamp,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

impl Timestamp {
    pub fn time_ms(&self) -> u64 {
        self.0
    }
}

pub trait Db: 'static + Send + Sync {
    /// These streams get new elements whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn new_objects(&self) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewObject>>;
    /// This function returns all new events for events on objects that have been
    /// subscribed on. Objects subscribed on are all the objects that have ever been
    /// created with `created`, or obtained with `get` or `query`, as well as all
    /// objects received through `new_objects`, excluding objects explicitly unsubscribed
    /// from
    fn new_events(&self) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewEvent>>;
    fn new_snapshots(
        &self,
    ) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewSnapshot>>;
    /// Note that this function unsubscribes ALL the streams that have ever been taken from this
    /// database; and purges it from the local database.
    fn unsubscribe(&self, ptr: ObjectId) -> impl Send + Future<Output = anyhow::Result<()>>;

    fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn create_all<T: Object>(
        &self,
        o: FullObject,
    ) -> impl Send + Future<Output = anyhow::Result<()>> {
        async move {
            let (creation, changes) = o.extract_all_clone();
            self.create::<T>(
                creation.id,
                creation.created_at,
                creation
                    .creation
                    .arc_to_any()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("API returned object of unexpected type"))?,
            )
            .await?;
            for (event_id, c) in changes.into_iter() {
                self.submit::<T>(
                    creation.id,
                    event_id,
                    c.event
                        .arc_to_any()
                        .downcast::<T::Event>()
                        .map_err(|_| anyhow!("API returned object of unexpected type"))?,
                )
                .await?;
            }
            Ok(())
        }
    }

    fn get<T: Object>(
        &self,
        ptr: ObjectId,
    ) -> impl Send + Future<Output = anyhow::Result<Option<FullObject>>>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> impl Send + Future<Output = anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>>>;

    fn snapshot<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

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
