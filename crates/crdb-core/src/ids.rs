use ulid::Ulid;

macro_rules! impl_id {
    ($type:ident) => {
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
        pub struct $type(#[educe(Debug(method(std::fmt::Display::fmt)))] pub Ulid);

        #[allow(dead_code)]
        impl $type {
            pub fn now() -> Self {
                Self(Ulid::new())
            }

            #[cfg(feature = "uuid")]
            pub fn to_uuid(self) -> uuid::Uuid {
                uuid::Uuid::from_bytes(self.0.to_bytes())
            }

            #[cfg(feature = "uuid")]
            pub fn from_uuid(id: uuid::Uuid) -> Self {
                Self(Ulid::from_bytes(*id.as_bytes()))
            }

            #[cfg(feature = "indexed-db")]
            pub fn to_js_string(&self) -> web_sys::js_sys::JsString {
                web_sys::js_sys::JsString::from(format!("{}", self.0))
            }

            pub fn from_u128(v: u128) -> Self {
                Self(Ulid::from_bytes(v.to_be_bytes()))
            }

            pub fn as_u128(&self) -> u128 {
                u128::from_be_bytes(self.0.to_bytes())
            }
        }

        #[cfg(feature = "sqlx-postgres")]
        impl<'q> sqlx::encode::Encode<'q, sqlx::Postgres> for $type {
            fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.to_uuid(), buf)
            }
            fn encode(self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::encode(self.to_uuid(), buf)
            }
            fn produces(&self) -> Option<sqlx::postgres::PgTypeInfo> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::produces(&self.to_uuid())
            }
            fn size_hint(&self) -> usize {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Postgres>>::size_hint(&self.to_uuid())
            }
        }

        #[cfg(feature = "sqlx-postgres")]
        impl sqlx::Type<sqlx::Postgres> for $type {
            fn type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
            }
            fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx-postgres")]
        impl sqlx::postgres::PgHasArrayType for $type {
            fn array_type_info() -> sqlx::postgres::PgTypeInfo {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
            }
            fn array_compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
                <uuid::Uuid as sqlx::postgres::PgHasArrayType>::array_compatible(ty)
            }
        }

        #[cfg(feature = "sqlx-sqlite")]
        impl<'q> sqlx::encode::Encode<'q, sqlx::Sqlite> for $type {
            fn encode_by_ref(&self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::encode_by_ref(&self.to_uuid(), buf)
            }
            fn encode(self, buf: &mut Vec<sqlx::sqlite::SqliteArgumentValue<'q>>) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::encode(self.to_uuid(), buf)
            }
            fn produces(&self) -> Option<sqlx::sqlite::SqliteTypeInfo> {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::produces(&self.to_uuid())
            }
            fn size_hint(&self) -> usize {
                <uuid::Uuid as sqlx::encode::Encode<'q, sqlx::Sqlite>>::size_hint(&self.to_uuid())
            }
        }

        #[cfg(feature = "sqlx-sqlite")]
        impl sqlx::Type<sqlx::Sqlite> for $type {
            fn type_info() -> sqlx::sqlite::SqliteTypeInfo {
                <uuid::Uuid as sqlx::Type<sqlx::Sqlite>>::type_info()
            }
            fn compatible(ty: &sqlx::sqlite::SqliteTypeInfo) -> bool {
                <uuid::Uuid as sqlx::Type<sqlx::Sqlite>>::compatible(ty)
            }
        }

        #[cfg(feature = "arbitrary")]
        impl<'a> arbitrary::Arbitrary<'a> for $type {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                Ok(Self(Ulid::from_bytes(u.arbitrary()?)))
            }
        }

        deepsize::known_deep_size!(0; $type); // These types does not allocate
    };
}

impl_id!(ObjectId);
impl_id!(EventId);
impl_id!(TypeId);
impl_id!(BinPtr);
impl_id!(QueryId);
impl_id!(User);
impl_id!(SessionRef);
impl_id!(SessionToken);
impl_id!(Updatedness);

impl SessionToken {
    #[cfg(feature = "server")]
    pub fn new() -> SessionToken {
        use rand::Rng;
        SessionToken(ulid::Ulid::from_bytes(rand::rng().random()))
    }
}

#[cfg(feature = "server")]
impl Default for SessionToken {
    fn default() -> SessionToken {
        SessionToken::new()
    }
}
