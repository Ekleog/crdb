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
        // TODO: also have a way to force a server round-trip NOW, for eg. permissions change.
        // This should probably be done by somehow exposing the queue for ApiDb
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

                pub fn [< unsubscribe_from_ $name >](&self, object: crdb::DbPtr<$object>) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                    self.db.unsubscribe(object.to_object_id())
                }

                pub fn [< create_ $name >](&self, object: crdb::Arc<$object>) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<crdb::DbPtr<$object>>> {
                    async move {
                        let id = crdb::Ulid::new();
                        self.db.create(crdb::ObjectId(id), crdb::EventId(id), object).await?;
                        Ok(crdb::DbPtr::from(crdb::ObjectId(id)))
                    }
                }

                pub fn [< submit_to_ $name >](&self, object: crdb::DbPtr<$object>, event: crdb::Arc<<$object as crdb::Object>::Event>) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<()>> {
                    // TODO: replace with an ulid generator, to make sure the id only ever goes UP
                    let id = crdb::Ulid::new();
                    self.db.submit::<$object>(object.to_object_id(), crdb::EventId(id), event)
                }

                pub fn [< get_ $name >](&self, object: crdb::DbPtr<$object>) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<Option<crdb::Arc<$object>>>> {
                    async move {
                        Ok(match self.db.get::<$object>(object.to_object_id()).await? {
                            Some(mut o) => Some(o.last_snapshot()?),
                            None => None,
                        })
                    }
                }

                pub fn [< query_ $name >](
                    &self,
                    include_heavy: bool,
                    ignore_not_modified_on_server_since: Option<crdb::Timestamp>,
                    q: crdb::Query,
                ) -> impl '_ + Send + crdb::Future<Output = crdb::anyhow::Result<impl '_ + Send + crdb::Stream<Item = anyhow::Result<crdb::Arc<$object>>>>> {
                    async move {
                        Ok(self.db.query::<$object>(
                            self.db.user(),
                            include_heavy,
                            ignore_not_modified_on_server_since,
                            q,
                        )
                        .await?
                        .map(|o| {
                            o?.last_snapshot()
                                .context("recovering the last snapshot of known-type object")
                        }))
                    }
                }
            })*
        }
    };
}
