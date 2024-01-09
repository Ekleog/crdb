use crate::{
    api::Query,
    full_object::{DynSized, FullObject},
    future::{CrdbSend, CrdbSync},
    BinPtr, CanDoCallbacks, CrdbFuture, CrdbStream, EventId, Object, ObjectId, TypeId, User,
};
use std::{sync::Arc, time::SystemTime};
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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(test, derive(bolero::generator::TypeGenerator))]
pub struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

impl Timestamp {
    pub fn now() -> Timestamp {
        Timestamp(
            u64::try_from(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap(),
        )
    }

    pub fn from_ms(v: u64) -> Timestamp {
        Timestamp(v)
    }

    pub fn max_for_ulid() -> Timestamp {
        Timestamp((1 << Ulid::TIME_BITS) - 1)
    }

    pub fn time_ms(&self) -> u64 {
        self.0
    }

    #[cfg(feature = "server")]
    pub fn from_i64_ms(v: i64) -> Timestamp {
        Timestamp(u64::try_from(v).expect("negative timestamp made its way in the database"))
    }

    #[cfg(feature = "server")]
    pub fn time_ms_i(&self) -> crate::Result<i64> {
        i64::try_from(self.0).map_err(|_| crate::Error::InvalidTimestamp(*self))
    }
}

pub trait Db: 'static + CrdbSend + CrdbSync {
    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn get<T: Object>(&self, ptr: ObjectId) -> impl CrdbFuture<Output = crate::Result<FullObject>>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> impl CrdbFuture<Output = anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>>>;

    fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn create_binary(
        &self,
        binary_id: BinPtr,
        data: Arc<Vec<u8>>,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
    fn get_binary(
        &self,
        binary_id: BinPtr,
    ) -> impl CrdbFuture<Output = anyhow::Result<Option<Arc<Vec<u8>>>>>;
}
