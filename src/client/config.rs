use crate::{
    db_trait::{EventId, Timestamp},
    DbPtr, Object,
};
use std::sync::Arc;

pub struct NewObject<T: Object> {
    pub ptr: DbPtr<T>,
    pub object: Arc<T>,
}

pub struct NewEvent<T: Object> {
    pub object: DbPtr<T>,
    pub id: EventId,
    pub event: Arc<T::Event>,
}

pub struct NewSnapshot<T: Object> {
    pub object: DbPtr<T>,
    pub time: Timestamp,
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ( $authenticator:ty | $api_config:ident | $client_db:ident | $($name:ident : $object:ty),* ) => {
        // TODO: generate something like an "impl" of client::Db that just forwards to the crate::Db impl of Cache<IndexedDbCache<Api>>
        // set_new_* MUST be replaced by one function for each object/event type, so that the user can properly handle them.
        // TODO: also have a way to force a server round-trip NOW, for eg. permissions change
        // TODO: use the async_broadcast crate with overflow disabled to fan-out in a blocking manner the new_object/event notifications
        pub struct $client_db {
            db: crdb::ClientDb<$authenticator>,
        }

        impl $client_db {
            pub fn connect(base_url: crdb::Arc<String>, auth: crdb::Arc<$authenticator>) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<$client_db>> {
                async move {
                    Ok($client_db {
                        db: crdb::ClientDb::connect::<$api_config>(base_url, auth).await?,
                    })
                }
            }

            pub fn disconnect(self) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                async move { todo!() }
            }

            pub fn create_binary(&self, value: crdb::Arc<Vec<u8>>) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                // TODO: compute sha224 limited to 16 bytes for BinPtr
                async move { todo!() }
            }

            pub fn get_binary(&self, id: crdb::BinPtr) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<Option<crdb::Arc<Vec<u8>>>>> {
                async move { todo!() }
            }

            $(crdb::paste! {
                pub fn [< new_ $name _objects >](&self) -> impl Send + crdb::Future<Output = impl Send + crdb::Stream<Item = $crate::NewObject<$object>>> {
                    async move {
                        // todo!()
                        crdb::futures::stream::empty()
                    }
                }

                pub fn [< new_ $name _events >](&self) -> impl Send + crdb::Future<Output = impl Send + crdb::Stream<Item = $crate::NewEvent<$object>>> {
                    async move {
                        // todo!()
                        crdb::futures::stream::empty()
                    }
                }

                pub fn [< new_ $name _snapshots >](&self) -> impl Send + crdb::Future<Output = impl Send + crdb::Stream<Item = $crate::NewSnapshot<$object>>> {
                    async move {
                        // todo!()
                        crdb::futures::stream::empty()
                    }
                }

                pub fn [< unsubscribe_from_ $name >](&self) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                    async move { todo!() }
                }

                pub fn [< create_ $name >](&self, object: crdb::Arc<$object>) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<crdb::DbPtr<$object>>> {
                    async move { todo!() }
                }

                pub fn [< submit_to_ $name >](&self, object: crdb::DbPtr<$object>, event: <$object as crdb::Object>::Event) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                    async move { todo!() }
                }

                pub fn [< get_ $name >](&self, object: crdb::DbPtr<$object>) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<crdb::Arc<$object>>> {
                    async move { todo!() }
                }

                pub fn [< query_ $name >](
                    &self,
                    include_heavy: bool,
                    ignore_not_modified_on_server_since: Option<crdb::Timestamp>,
                    q: crdb::Query,
                ) -> impl Send + crdb::Future<Output = crdb::anyhow::Result<impl crdb::Stream<Item = crdb::Arc<$object>>>> {
                    async move {
                        // todo!()
                        Ok(crdb::futures::stream::empty())
                    }
                }
            })*
        }
    };
}
