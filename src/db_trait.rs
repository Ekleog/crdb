use crate::{
    api::{BinPtr, Query},
    full_object::{DynSized, FullObject},
    CanDoCallbacks, Object, User,
};
use anyhow::anyhow;
use futures::Stream;
use std::{future::Future, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, educe::Educe)]
#[educe(Debug)]
pub struct ObjectId(#[educe(Debug(method(std::fmt::Display::fmt)))] pub Ulid);
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, educe::Educe)]
#[educe(Debug)]
pub struct EventId(#[educe(Debug(method(std::fmt::Display::fmt)))] pub Ulid);
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, educe::Educe)]
#[educe(Debug)]
pub struct TypeId(#[educe(Debug(method(std::fmt::Display::fmt)))] pub Ulid);

macro_rules! impl_for_id {
    ($type:ty) => {
        #[allow(dead_code)]
        impl $type {
            pub(crate) fn time(&self) -> Timestamp {
                Timestamp(self.0.timestamp_ms())
            }

            #[cfg(feature = "server")]
            pub(crate) fn to_uuid(&self) -> uuid::Uuid {
                uuid::Uuid::from_bytes(self.0.to_bytes())
            }

            #[cfg(feature = "server")]
            pub(crate) fn from_uuid(id: uuid::Uuid) -> Self {
                Self(Ulid::from_bytes(*id.as_bytes()))
            }

            pub(crate) fn last_id_at(time: Timestamp) -> Self {
                Self(Ulid::from_parts(time.time_ms(), (1 << Ulid::RAND_BITS) - 1))
            }

            pub(crate) fn from_u128(v: u128) -> Self {
                Self(Ulid::from_bytes(v.to_be_bytes()))
            }

            pub(crate) fn as_u128(&self) -> u128 {
                u128::from_be_bytes(self.0.to_bytes())
            }
        }

        #[cfg(feature = "server")]
        impl<'q> sqlx::encode::Encode<'q, sqlx::Postgres> for $type {
            fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.to_uuid(), buf)
            }
            fn encode(self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode(self.to_uuid(), buf)
            }
            fn produces(&self) -> Option<sqlx::postgres::PgTypeInfo> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::produces(&self.to_uuid())
            }
            fn size_hint(&self) -> usize {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::size_hint(&self.to_uuid())
            }
        }

        #[cfg(feature = "server")]
        impl sqlx::Type<sqlx::Postgres> for $type {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(test)]
        impl bolero::generator::TypeGenerator for $type {
            fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Self> {
                Some(Self(Ulid::from_bytes(<[u8; 16] as bolero::generator::TypeGenerator>::generate::<D>(driver)?)))
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

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DbOpError {
    #[error("Missing binary pointers: {0:?}")]
    MissingBinPtrs(Vec<BinPtr>),

    #[error(transparent)]
    Other(anyhow::Error),
}

impl DbOpError {
    pub fn with_context<F: FnOnce() -> String>(self, f: F) -> DbOpError {
        match self {
            DbOpError::MissingBinPtrs(b) => DbOpError::MissingBinPtrs(b),
            DbOpError::Other(e) => DbOpError::Other(e.context(f())),
        }
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
    fn new_recreations(
        &self,
    ) -> impl Send + Future<Output = impl Send + Stream<Item = DynNewRecreation>>;
    /// Note that this function unsubscribes ALL the streams that have ever been taken from this
    /// database; and purges it from the local database.
    fn unsubscribe(&self, ptr: ObjectId) -> impl Send + Future<Output = anyhow::Result<()>>;

    fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> impl Send + Future<Output = Result<(), DbOpError>>;
    fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn create_all<T: Object, C: CanDoCallbacks>(
        &self,
        o: FullObject,
        cb: &C,
    ) -> impl Send + Future<Output = anyhow::Result<()>> {
        async move {
            let (creation, changes) = o.extract_all_clone();
            self.create::<T, _>(
                creation.id,
                creation.created_at,
                creation
                    .creation
                    .arc_to_any()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("API returned object of unexpected type"))?,
                cb,
            )
            .await?;
            for (event_id, c) in changes.into_iter() {
                self.submit::<T, _>(
                    creation.id,
                    event_id,
                    c.event
                        .arc_to_any()
                        .downcast::<T::Event>()
                        .map_err(|_| anyhow!("API returned object of unexpected type"))?,
                    cb,
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

    fn recreate<T: Object>(
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
