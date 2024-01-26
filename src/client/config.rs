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
    ( $api_config:ident | $client_db:ident | $($name:ident : $object:ty),* ) => {
        // TODO(api): also have a way to force a server round-trip NOW, for eg. permissions change.
        // This should probably be done by somehow exposing the queue for ApiDb
        pub struct $client_db {
            db: crdb::ClientDb,
            ulid: crdb::Mutex<crdb::ulid::Generator>,
        }

        impl $client_db {
            pub fn connect<F: 'static + Send + Fn(crdb::ClientStorageInfo) -> bool>(
                local_db: String,
                cache_watermark: usize,
                vacuum_schedule: crdb::ClientVacuumSchedule<F>,
            ) -> impl crdb::CrdbFuture<Output = crdb::anyhow::Result<$client_db>> {
                async move {
                    Ok($client_db {
                        db: crdb::ClientDb::new::<$api_config, F>(&local_db, cache_watermark, vacuum_schedule).await?,
                        ulid: crdb::Mutex::new(crdb::ulid::Generator::new()),
                    })
                }
            }

            /// `cb` will be called with the parameter `true` if we just connected (again), and `false` if
            /// we just noticed a disconnection.
            pub fn on_connection_event(&self, cb: impl 'static + Send + Sync + Fn(crdb::ConnectionEvent)) {
                self.db.on_connection_event(cb)
            }

            pub fn login(&self, url: crdb::Arc<String>, token: crdb::SessionToken) {
                self.db.login(url, token)
            }

            pub fn logout(&self) -> impl '_ + crdb::CrdbFuture<Output = crdb::anyhow::Result<()>> {
                self.db.logout()
            }

            /// Pauses the vacuum until the returned mutex guard is dropped
            pub fn pause_vacuum(&self) -> impl '_ + crdb::CrdbFuture<Output = crdb::tokio::sync::RwLockReadGuard<'_, ()>> {
                self.db.pause_vacuum()
            }

            /// Note: when creating a binary, it can be vacuumed away any time until an object or
            /// event is added that requires it. As such, you probably want to use `pause_vacuum`
            /// to make sure the created binary is not vacuumed away before the object or event
            /// had enough time to get created.
            pub fn create_binary(&self, data: crdb::Arc<[u8]>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                let binary_id = crdb::hash_binary(&*data);
                self.db.create_binary(binary_id, data)
            }

            pub fn get_binary(&self, binary_id: crdb::BinPtr) -> impl '_ + crdb::CrdbFuture<Output = crdb::anyhow::Result<Option<crdb::Arc<[u8]>>>> {
                self.db.get_binary(binary_id)
            }

            /// To lock, use `get` with the `lock` argument set to `true`
            pub fn unlock<T: crdb::Object>(&self, object: crdb::DbPtr<T>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                self.db.unlock(object.to_object_id())
            }

            pub fn unsubscribe<T: crdb::Object>(&self, object: crdb::DbPtr<T>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                let mut set = std::collections::HashSet::new();
                set.insert(object.to_object_id());
                self.db.unsubscribe(set)
            }

            pub fn listen_for_updates(&self) -> crdb::tokio::sync::broadcast::Receiver<crdb::ObjectId> {
                self.db.listen_for_updates()
            }

            $(crdb::paste! {
                pub fn [< create_ $name >](
                    &self,
                    object: crdb::Arc<$object>,
                ) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<(crdb::DbPtr<$object>, impl crdb::CrdbFuture<Output = crdb::Result<()>>)>> {
                    async move {
                        let id = self.ulid.lock().unwrap().generate();
                        let id = id.expect("Failed to generate ulid for object creation");
                        let completion = self.db.create(crdb::ObjectId(id), crdb::EventId(id), object).await?;
                        Ok((crdb::DbPtr::from(crdb::ObjectId(id)), completion))
                    }
                }

                pub fn [< submit_to_ $name >](
                    &self,
                    object: crdb::DbPtr<$object>,
                    event: crdb::Arc<<$object as crdb::Object>::Event>,
                ) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<impl crdb::CrdbFuture<Output = crdb::Result<()>>>> {
                    let id = self.ulid.lock().unwrap().generate();
                    let id = id.expect("Failed to generate ulid for event submission");
                    self.db.submit::<$object>(object.to_object_id(), crdb::EventId(id), event)
                }

                pub fn [< get_ $name >](&self, lock: bool, object: crdb::DbPtr<$object>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<crdb::Arc<$object>>> {
                    self.db.get::<$object>(lock, object.to_object_id())
                }

                pub fn [< query_ $name _local >]<'a>(&'a self, lock: bool, query: &'a crdb::Query) -> impl 'a + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    self.db.query_local::<$object>(lock, query)
                }

                // TODO(low): does this user-exposed function really need `ignore_not_modified_on_server_since`?
                // Can we not hide it by subscribing not only to individual objects, but to Query's so that crdb
                // itself could handle that internally? Or maybe even just by fetching all objects that are
                // readable and were created since the last connection upon login?
                pub fn [< query_ $name _remote >]<'a>(
                    &'a self,
                    lock: bool,
                    only_updated_since: Option<crdb::Timestamp>,
                    query: &'a crdb::Query,
                ) -> impl 'a + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    self.db.query_remote::<$object>(
                        lock,
                        only_updated_since,
                        query,
                    )
                }
            })*
        }
    };
}
