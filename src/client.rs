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
        struct $client_db {
            // TODO
        }

        impl $client_db {
            pub async fn connect(_base_url: String, auth: &$authenticator) -> $client_db {
                todo!()
            }

            $(crdb_internal::paste! {
                pub fn [< new_ $name _objects >](&self) -> impl Send + crdb_internal::Future<Output = impl Send + crdb_internal::Stream<Item = $crate::NewObject<$object>>> {
                    async move {
                        // todo!()
                        crdb_internal::futures::stream::empty()
                    }
                }

                pub fn [< new_ $name _events >](&self) -> impl Send + crdb_internal::Future<Output = impl Send + crdb_internal::Stream<Item = $crate::NewEvent<$object>>> {
                    async move {
                        // todo!()
                        crdb_internal::futures::stream::empty()
                    }
                }

                pub fn [< new_ $name _snapshots >](&self) -> impl Send + crdb_internal::Future<Output = impl Send + crdb_internal::Stream<Item = $crate::NewSnapshot<$object>>> {
                    async move {
                        // todo!()
                        crdb_internal::futures::stream::empty()
                    }
                }

                pub fn [< create_ $name >](&self, object: crdb_internal::Arc<$object>) -> impl Send + crdb_internal::Future<Output = crdb_internal::anyhow::Result<crdb_internal::DbPtr<$object>>> {
                    async move { todo!() }
                }

                pub fn [< submit_to_ $name >](&self, object: crdb_internal::DbPtr<$object>, event: <$object as crdb_internal::Object>::Event) -> impl Send + crdb_internal::Future<Output = crdb_internal::anyhow::Result<()>> {
                    async move { todo!() }
                }
            })*
        }
    };
}
