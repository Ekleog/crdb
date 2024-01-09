use super::postgres_db::{ComboLock, PostgresDb};
use crate::{api::ApiConfig, CanDoCallbacks, CrdbFuture, ObjectId, Timestamp, TypeId, User};

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait ServerConfig: 'static + Sized + Send + Sync + crate::private::Sealed {
    type Auth;

    type ApiConfig: ApiConfig;

    fn get_users_who_can_read<'a, C: CanDoCallbacks>(
        call_on: &'a PostgresDb<Self>,
        object_id: ObjectId,
        type_id: TypeId,
        snapshot_version: i32,
        snapshot: serde_json::Value,
        cb: &'a C,
    ) -> impl 'a + CrdbFuture<Output = crate::Result<(Vec<User>, Vec<ObjectId>, Vec<ComboLock<'a>>)>>;

    fn recreate<'a, C: CanDoCallbacks>(
        call_on: &'a PostgresDb<Self>,
        type_id: TypeId,
        object_id: ObjectId,
        time: Timestamp,
        cb: &'a C,
    ) -> impl 'a + CrdbFuture<Output = crate::Result<bool>>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_server {
    ( $auth:ty | $api_config:ident | $name:ident | $($object:ty),* ) => {
        pub struct $name;

        impl crdb::private::Sealed for $name {}
        impl crdb::ServerConfig for $name {
            type Auth = $auth;
            type ApiConfig = $api_config;

            async fn get_users_who_can_read<'a, C: crdb::CanDoCallbacks>(
                call_on: &'a crdb::PostgresDb<Self>,
                object_id: crdb::ObjectId,
                type_id: crdb::TypeId,
                snapshot_version: i32,
                snapshot: crdb::serde_json::Value,
                cb: &'a C,
            ) -> crdb::Result<(Vec<crdb::User>, Vec<crdb::ObjectId>, Vec<crdb::ComboLock<'a>>)> {
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let snapshot = crdb::parse_snapshot::<$object>(snapshot_version, snapshot)
                            .wrap_with_context(|| format!("parsing snapshot for {object_id:?}"))?;
                        let res = call_on.get_users_who_can_read(&object_id, &snapshot, cb).await
                            .wrap_with_context(|| format!("listing users who can read {object_id:?}"))?;
                        return Ok(res);
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }

            async fn recreate<'a, C: crdb::CanDoCallbacks>(
                call_on: &'a crdb::PostgresDb<Self>,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                time: crdb::Timestamp,
                cb: &'a C,
            ) -> crdb::Result<bool> {
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        return call_on.recreate_impl::<$object, C>(time, object_id, cb).await;
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }
        }
    };
}
