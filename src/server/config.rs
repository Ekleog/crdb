use super::postgres_db::PostgresDb;
use crate::{api::ApiConfig, db_trait::TypeId, CanDoCallbacks};
use std::future::Future;

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait ServerConfig: 'static + Sized + Send + Sync + crate::private::Sealed {
    type Auth;

    type ApiConfig: ApiConfig;

    fn update_users_who_can_read<'a, C: CanDoCallbacks>(
        call_on: &'a PostgresDb<Self>,
        type_id: TypeId,
        snapshot_version: i32,
        snapshot: serde_json::Value,
        cb: &'a C,
    ) -> impl 'a + Send + Future<Output = anyhow::Result<()>>;
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

            async fn update_users_who_can_read<'a, C: crdb::CanDoCallbacks>(
                call_on: &'a crdb::PostgresDb<Self>,
                type_id: crdb::TypeId,
                snapshot_version: i32,
                snapshot: crdb::serde_json::Value,
                cb: &'a C,
            ) -> crdb::anyhow::Result<()> {
                $(
                    if type_id == crdb::TypeId(*<$object as crdb::Object>::type_ulid()) {
                        let snapshot = crdb::parse_snapshot::<$object>(snapshot_version, snapshot)
                            .context("parsing the snapshot")?;
                        call_on.update_users_who_can_read(&snapshot, cb).await?;
                        return Ok(());
                    }
                )*
                Err(crdb::anyhow::anyhow!("Unknown type ID: {type_id:?}"))
            }
        }
    };
}
