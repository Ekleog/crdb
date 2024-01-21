use crate::{DbPtr, EventId, Object, Timestamp};
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

pub struct NewRecreation<T: Object> {
    pub object: DbPtr<T>,
    pub time: Timestamp,
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ( $authenticator:ty | $api_config:ident | $client_db:ident | $($name:ident : $object:ty),* ) => {
        // TODO(api): also have a way to force a server round-trip NOW, for eg. permissions change.
        // This should probably be done by somehow exposing the queue for ApiDb
        pub struct $client_db {
            db: crdb::ClientDb<$authenticator>,
            ulid: crdb::Mutex<crdb::ulid::Generator>,
        }

        impl $client_db {
            pub fn connect<F: 'static + Send + Fn(crdb::ClientStorageInfo) -> bool>(
                base_url: crdb::Arc<String>,
                auth: crdb::Arc<$authenticator>,
                local_db: String,
                cache_watermark: usize,
                vacuum_schedule: crdb::ClientVacuumSchedule<F>,
            ) -> impl crdb::CrdbFuture<Output = crdb::anyhow::Result<$client_db>> {
                async move {
                    Ok($client_db {
                        db: crdb::ClientDb::connect::<$api_config, F>(base_url, auth, &local_db, cache_watermark, vacuum_schedule).await?,
                        ulid: crdb::Mutex::new(crdb::ulid::Generator::new()),
                    })
                }
            }

            pub fn disconnect(&self) -> impl '_ + crdb::CrdbFuture<Output = crdb::anyhow::Result<()>> {
                self.db.disconnect()
            }

            /// Pauses the vacuum until the returned mutex guard is dropped
            pub fn pause_vacuum(&self) -> impl '_ + crdb::CrdbFuture<Output = crdb::tokio::sync::RwLockReadGuard<'_, ()>> {
                self.db.pause_vacuum()
            }

            /// Note: when creating a binary, it can be vacuumed away any time until an object or
            /// event is added that requires it. As such, you probably want to use `pause_vacuum`
            /// to make sure the created binary is not vacuumed away before the object or event
            /// had enough time to get created.
            pub fn create_binary(&self, data: crdb::Arc<Vec<u8>>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                let binary_id = crdb::hash_binary(&*data);
                self.db.create_binary(binary_id, data)
            }

            pub fn get_binary(&self, binary_id: crdb::BinPtr) -> impl '_ + crdb::CrdbFuture<Output = crdb::anyhow::Result<Option<crdb::Arc<Vec<u8>>>>> {
                self.db.get_binary(binary_id)
            }

            /// To lock, use `get` with the `lock` argument set to `true`
            pub fn unlock<T: crdb::Object>(&self, object: crdb::DbPtr<T>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                self.db.unlock(object.to_object_id())
            }

            pub fn unsubscribe<T: crdb::Object>(&self, object: crdb::DbPtr<T>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                self.db.unsubscribe(object.to_object_id())
            }

            $(crdb::paste! {
                pub fn [< new_ $name _objects >](&self) -> impl '_ + crdb::CrdbFuture<Output = impl '_ + crdb::CrdbStream<Item = $crate::NewObject<$object>>> {
                    async move {
                        self.db
                            .new_objects()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id == *<$object as crdb::Object>::type_ulid()))
                            .map(|o| $crate::NewObject {
                                ptr: crdb::DbPtr::from(o.id),
                                object: o.object.arc_to_any().downcast::<$object>()
                                    .expect("Failed downcasting object with checked type id")
                            })
                    }
                }

                pub fn [< new_ $name _events >](&self) -> impl '_ + crdb::CrdbFuture<Output = impl '_ + crdb::CrdbStream<Item = $crate::NewEvent<$object>>> {
                    async move {
                        self.db
                            .new_events()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id == *<$object as crdb::Object>::type_ulid()))
                            .map(|o| $crate::NewEvent {
                                object: crdb::DbPtr::from(o.object_id),
                                id: o.id,
                                event: o.event.arc_to_any().downcast::<<$object as crdb::Object>::Event>()
                                    .expect("Failed downcasting event with checked type id")
                            })
                    }
                }

                pub fn [< new_ $name _recreations >](&self) -> impl '_ + crdb::CrdbFuture<Output = impl '_ + Send + crdb::Stream<Item = $crate::NewRecreation<$object>>> {
                    async move {
                        self.db
                            .new_recreations()
                            .await
                            .filter(|o| crdb::future::ready(o.type_id == *<$object as crdb::Object>::type_ulid()))
                            .map(|o| $crate::NewRecreation {
                                object: crdb::DbPtr::from(o.object_id),
                                time: o.time,
                            })
                    }
                }

                pub fn [< create_ $name >](&self, object: crdb::Arc<$object>) -> impl '_ + crdb::CrdbFuture<Output = crdb::anyhow::Result<crdb::DbPtr<$object>>> {
                    async move {
                        let id = self.ulid.lock().unwrap().generate();
                        let id = id.expect("Failed to generate ulid for object creation");
                        self.db.create(crdb::ObjectId(id), crdb::EventId(id), object).await?;
                        Ok(crdb::DbPtr::from(crdb::ObjectId(id)))
                    }
                }

                pub fn [< submit_to_ $name >](&self, object: crdb::DbPtr<$object>, event: crdb::Arc<<$object as crdb::Object>::Event>) -> impl '_ + crdb::CrdbFuture<Output = Result<(), crdb::Error>> {
                    let id = self.ulid.lock().unwrap().generate();
                    let id = id.expect("Failed to generate ulid for event submission");
                    self.db.submit::<$object>(object.to_object_id(), crdb::EventId(id), event)
                }

                pub fn [< get_ $name >](&self, lock: bool, object: crdb::DbPtr<$object>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<crdb::Arc<$object>>> {
                    async move {
                        self.db.get::<$object>(lock, object.to_object_id()).await?.last_snapshot()
                            .wrap_with_context(|| format!("getting last snapshot of {object:?}"))
                    }
                }

                pub fn [< query_ $name _local >]<'a>(&'a self, q: &'a crdb::Query) -> impl 'a + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    async move {
                        Ok(self.db.query_local::<$object>(self.db.user(), q)
                            .await?
                            .then(|o| async move {
                                o?.last_snapshot()
                                    .wrap_context("recovering the last snapshot of known-type object")
                            }))
                    }
                }

                // TODO(low): does this user-exposed function really need `ignore_not_modified_on_server_since`?
                // Can we not hide it by subscribing not only to individual objects, but to Query's so that crdb
                // itself could handle that internally? Or maybe even just by fetching all objects that are
                // readable and were created since the last connection upon login?
                pub fn [< query_ $name _remote >]<'a>(
                    &'a self,
                    lock: bool,
                    ignore_not_modified_on_server_since: Option<crdb::Timestamp>,
                    q: &'a crdb::Query,
                ) -> impl 'a + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    async move {
                        Ok(self.db.query_remote::<$object>(
                            lock,
                            ignore_not_modified_on_server_since,
                            q,
                        )
                        .await?
                        .then(|o| async move {
                            o?.last_snapshot()
                                .wrap_context("recovering the last snapshot of known-type object")
                        }))
                    }
                }
            })*
        }
    };
}
