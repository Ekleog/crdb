use crate::Timestamp;
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

#[derive(
    Clone,
    Copy,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    educe::Educe,
    serde::Deserialize,
    serde::Serialize,
)]
#[educe(Debug)]
pub struct BinPtr(#[educe(Debug(method(std::fmt::Display::fmt)))] pub(crate) Ulid);

#[derive(
    Clone,
    Copy,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    educe::Educe,
    serde::Deserialize,
    serde::Serialize,
)]
#[educe(Debug)]
pub struct User(#[educe(Debug(method(std::fmt::Display::fmt)))] pub Ulid);

macro_rules! impl_for_id {
    ($type:ty) => {
        #[allow(dead_code)]
        impl $type {
            pub(crate) fn now() -> Self {
                Self(Ulid::new())
            }

            pub(crate) fn time(&self) -> Timestamp {
                Timestamp::from_ms(self.0.timestamp_ms())
            }

            #[cfg(feature = "server")]
            pub(crate) fn to_uuid(&self) -> uuid::Uuid {
                uuid::Uuid::from_bytes(self.0.to_bytes())
            }

            #[cfg(feature = "server")]
            pub(crate) fn from_uuid(id: uuid::Uuid) -> Self {
                Self(Ulid::from_bytes(*id.as_bytes()))
            }

            pub(crate) fn last_id_at(time: Timestamp) -> crate::Result<Self> {
                if time.time_ms() >= (1 << Ulid::TIME_BITS) {
                    return Err(crate::Error::InvalidTimestamp(time));
                }
                Ok(Self(Ulid::from_parts(time.time_ms(), (1 << Ulid::RAND_BITS) - 1)))
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

        #[cfg(feature = "server")]
        impl sqlx::postgres::PgHasArrayType for $type {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
            fn array_compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_compatible(ty)
            }
        }

        #[cfg(feature = "client-native")]
        impl<'q> sqlx::encode::Encode<'q, sqlx::Sqlite> for $type {
            fn encode_by_ref(&self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::encode_by_ref(&self.to_uuid(), buf)
            }
            fn encode(self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>) -> sqlx::encode::IsNull {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::encode(self.to_uuid(), buf)
            }
            fn produces(&self) -> Option<sqlx::sqlite::SqliteTypeInfo> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::produces(&self.to_uuid())
            }
            fn size_hint(&self) -> usize {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::size_hint(&self.to_uuid())
            }
        }

        #[cfg(feature = "client-native")]
        impl sqlx::Type<sqlx::Sqlite> for $type {
            fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Sqlite>>::type_info()
            }
            fn compatible(ty: &sqlx::sqlite::SqliteTypeInfo) -> bool {
                <uuid::Uuid as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
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

pub(crate) use impl_for_id;

impl_for_id!(ObjectId);
impl_for_id!(EventId);
impl_for_id!(TypeId);
impl_for_id!(BinPtr);
impl_for_id!(User);
