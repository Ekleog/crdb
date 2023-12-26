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
                self.db.disconnect()
            }

            pub fn create_binary(&self, value: crdb::Arc<Vec<u8>>) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                let id = crdb::hash_binary(&*value);
                self.db.create_binary(id, value)
            }

            pub fn get_binary(&self, id: crdb::BinPtr) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<Option<crdb::Arc<Vec<u8>>>>> {
                self.db.get_binary(id)
            }

            $(crdb::paste! {
                pub fn [< new_ $name _objects >](&self) -> impl '_ + Send + crdb::Future<Output = impl '_ + Send + crdb::Stream<Item = $crate::NewObject<$object>>> {
                    async move {
                        self.db
                            .new_objects()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id.0 == *<$object as crdb::Object>::ulid()))
                            .map(|o| $crate::NewObject {
                                ptr: crdb::DbPtr::from(o.id),
                                object: o.object.downcast::<$object>()
                                    .expect("Failed downcasting object with checked type id")
                            })
                    }
                }

                pub fn [< new_ $name _events >](&self) -> impl '_ + Send + crdb::Future<Output = impl '_ + Send + crdb::Stream<Item = $crate::NewEvent<$object>>> {
                    async move {
                        self.db
                            .new_events()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id.0 == *<$object as crdb::Object>::ulid()))
                            .map(|o| $crate::NewEvent {
                                object: crdb::DbPtr::from(o.object_id),
                                id: o.id,
                                event: o.event.downcast::<<$object as crdb::Object>::Event>()
                                    .expect("Failed downcasting event with checked type id")
                            })
                    }
                }

                pub fn [< new_ $name _snapshots >](&self) -> impl '_ + Send + crdb::Future<Output = impl '_ + Send + crdb::Stream<Item = $crate::NewSnapshot<$object>>> {
                    async move {
                        self.db
                            .new_snapshots()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id.0 == *<$object as crdb::Object>::ulid()))
                            .map(|o| $crate::NewSnapshot {
                                object: crdb::DbPtr::from(o.object_id),
                                time: o.time,
                            })
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
