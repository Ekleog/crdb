#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ( $api_config:ident | $client_db:ident | $($name:ident : $object:ty),* ) => {
        pub struct $client_db {
            db: crdb::ClientDb,
            ulid: crdb::Mutex<crdb::ulid::Generator>,
        }

        impl $client_db
        {
            pub fn connect<EH, EHF, CVS>(
                user: crdb::User,
                local_db: String,
                cache_watermark: usize,
                error_handler: EH,
                vacuum_schedule: crdb::ClientVacuumSchedule<CVS>,
            ) -> impl crdb::CrdbFuture<Output = crdb::anyhow::Result<(
                $client_db,
                impl crdb::CrdbFuture<Output = usize>,
            )>>
            where
                EH: 'static + Send + Sync + Fn(crdb::Upload, crdb::Error) -> EHF,
                EHF: 'static + crdb::CrdbFuture<Output = crdb::OnError>,
                CVS: 'static + Send + Fn(crdb::ClientStorageInfo) -> bool,
            {
                async move {
                    let (db, upgrade_handle) = crdb::ClientDb::new::<$api_config, EH, EHF, CVS>(user, &local_db, cache_watermark, error_handler, vacuum_schedule).await?;
                    Ok(($client_db {
                        db,
                        ulid: crdb::Mutex::new(crdb::ulid::Generator::new()),
                    }, upgrade_handle))
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

            pub fn watch_upload_queue(&self) -> crdb::tokio::sync::watch::Receiver<Vec<crdb::UploadId>> {
                self.db.watch_upload_queue()
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

            pub fn unsubscribe_query(&self, query: crdb::QueryId) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>> {
                self.db.unsubscribe_query(query)
            }

            pub fn listen_for_all_updates(&self) -> crdb::tokio::sync::broadcast::Receiver<crdb::ObjectId> {
                self.db.listen_for_all_updates()
            }

            pub fn listen_for_updates_on(&self, query_id: crdb::QueryId) -> Option<crdb::tokio::sync::broadcast::Receiver<crdb::ObjectId>> {
                self.db.listen_for_updates_on(query_id)
            }

            $(crdb::paste! {
                pub fn [< create_ $name >](
                    &self,
                    importance: crdb::Importance,
                    object: crdb::Arc<$object>,
                ) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<(crdb::DbPtr<$object>, impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>>)>> {
                    async move {
                        let id = self.ulid.lock().unwrap().generate();
                        let id = id.expect("Failed to generate ulid for object creation");
                        let completion = self.db.create(importance, crdb::ObjectId(id), crdb::EventId(id), object).await?;
                        Ok((crdb::DbPtr::from(crdb::ObjectId(id)), completion))
                    }
                }

                pub fn [< submit_to_ $name >](
                    &self,
                    importance: crdb::Importance,
                    object: crdb::DbPtr<$object>,
                    event: crdb::Arc<<$object as crdb::Object>::Event>,
                ) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbFuture<Output = crdb::Result<()>>>> {
                    let id = self.ulid.lock().unwrap().generate();
                    let id = id.expect("Failed to generate ulid for event submission");
                    self.db.submit::<$object>(importance, object.to_object_id(), crdb::EventId(id), event)
                }

                pub fn [< get_ $name >](&self, importance: crdb::Importance, object: crdb::DbPtr<$object>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<crdb::Arc<$object>>> {
                    self.db.get::<$object>(importance, object.to_object_id())
                }

                pub fn [< query_ $name _local >](&self, query: crdb::Arc<crdb::Query>) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    self.db.query_local::<$object>(query)
                }

                /// Note that it is assumed here that the same QueryId will always be associated with the same Query.
                /// In particular, this means that when bumping an Object's snapshot_version and adjusting the queries accordingly,
                /// you should change the QueryId, as well as unsubscribe/resubscribe on startup so that the database gets updated.
                ///
                /// `query_id` is ignored when `importance` is `Latest`.
                pub fn [< query_ $name _remote >](
                    &self,
                    importance: crdb::Importance,
                    query_id: crdb::QueryId,
                    query: crdb::Arc<crdb::Query>,
                ) -> impl '_ + crdb::CrdbFuture<Output = crdb::Result<impl '_ + crdb::CrdbStream<Item = crdb::Result<crdb::Arc<$object>>>>> {
                    self.db.query_remote::<$object>(
                        importance,
                        query_id,
                        query,
                    )
                }
            })*
        }
    };
}
