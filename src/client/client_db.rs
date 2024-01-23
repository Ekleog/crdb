use super::{api_db::ConnectionState, ApiDb, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp, User,
};
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

pub struct ClientDb {
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    vacuum_guard: Arc<RwLock<()>>,
}

impl ClientDb {
    pub async fn new<C: ApiConfig, F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        base_url: Arc<String>,
        local_db: &str,
        cache_watermark: usize,
        vacuum_schedule: ClientVacuumSchedule<F>,
    ) -> anyhow::Result<ClientDb> {
        C::check_ulids();
        let api = Arc::new(ApiDb::new(base_url));
        let db_bypass = Arc::new(LocalDb::connect(local_db).await?);
        let db = CacheDb::new(db_bypass.clone(), cache_watermark);
        let this = ClientDb {
            api,
            db,
            db_bypass,
            vacuum_guard: Arc::new(RwLock::new(())),
        };
        this.setup_watchers::<C>();
        this.setup_autovacuum(vacuum_schedule);
        Ok(this)
    }

    fn setup_watchers<C: ApiConfig>(&self) {
        use futures::pin_mut;

        // Watch new objects
        crate::spawn({
            let api = self.api.clone();
            let local_db = self.db.clone();
            async move {
                let new_objects = api.new_objects().await;
                pin_mut!(new_objects);
                while let Some(o) = new_objects.next().await {
                    let object = o.id;
                    if let Err(error) = C::create(&*local_db, o.clone(), false, &*local_db).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed creating received object in internal db"
                        );
                    }
                }
            }
        });

        // Watch new events
        crate::spawn({
            let api = self.api.clone();
            let local_db = self.db.clone();
            async move {
                let new_events = api.new_events().await;
                pin_mut!(new_events);
                while let Some(e) = new_events.next().await {
                    let object = e.object_id;
                    let event = e.id;
                    if let Err(error) = C::submit(&*local_db, e.clone(), &*local_db).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            ?event,
                            "failed submitting received object to internal db"
                        );
                    }
                    // DO NOT re-fetch object when receiving an event not in cache for it.
                    // Without this, users would risk unsubscribing from an object, then receiving
                    // an event on this object (as a race condition), and then staying subscribed.
                }
            }
        });

        // Watch new re-creations
        crate::spawn({
            let api = self.api.clone();
            let local_db = self.db.clone();
            async move {
                let new_recreations = api.new_recreations().await;
                pin_mut!(new_recreations);
                while let Some(s) = new_recreations.next().await {
                    let object = s.object_id;
                    if let Err(error) = C::recreate(&*local_db, s.clone(), &*local_db).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed recreating as per received event in internal db"
                        );
                    }
                }
            }
        });
    }

    fn setup_autovacuum<F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        &self,
        vacuum_schedule: ClientVacuumSchedule<F>,
    ) {
        let db_bypass = self.db_bypass.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        crate::spawn(async move {
            loop {
                match db_bypass.storage_info().await {
                    Ok(storage_info) => {
                        if (vacuum_schedule.filter)(storage_info) {
                            let _lock = vacuum_guard.write().await;
                            if let Err(err) = db_bypass.vacuum().await {
                                tracing::error!(?err, "error occurred while vacuuming");
                            }
                        }
                    }
                    Err(err) => tracing::error!(
                        ?err,
                        "failed recovering storage info to check for vacuumability"
                    ),
                };
            }
        });
    }

    /// `cb` will be called with the parameter `true` if we just connected (again), and `false` if
    /// we just noticed a disconnection.
    pub fn on_connection_state_change(&self, cb: impl 'static + Fn(ConnectionState)) {
        self.api.on_connection_state_change(cb)
    }

    pub fn login(&self, token: SessionToken) {
        self.api.login(token)
    }

    pub async fn logout(&self) -> anyhow::Result<()> {
        // TODO(api): flush self.db
        self.api.logout().await
    }

    pub async fn pause_vacuum(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.vacuum_guard.read().await
    }

    pub async fn clear_cache(&self) {
        self.db.clear_cache().await
    }

    pub async fn clear_binaries_cache(&self) {
        self.db.clear_binaries_cache().await
    }

    pub async fn clear_objects_cache(&self) {
        self.db.clear_objects_cache().await
    }

    pub async fn reduce_size_to(&mut self, size: usize) {
        self.db.reduce_size_to(size).await
    }

    pub async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        self.api.new_objects().await
    }

    pub async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        self.api.new_events().await
    }

    pub async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        self.api.new_recreations().await
    }

    pub async fn unlock(&self, ptr: ObjectId) -> crate::Result<()> {
        self.db.unlock(ptr).await
    }

    pub async fn unsubscribe(&self, ptr: ObjectId) -> crate::Result<()> {
        // TODO(client): automatically call `api.unsubscribe` when `vacuum` removes an object
        self.db.remove(ptr).await?;
        self.api.unsubscribe(ptr).await
    }

    pub async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<()> {
        self.api.create(id, created_at, object.clone()).await?;
        self.db
            .create(id, created_at, object, true, &*self.db)
            .await
    }

    pub async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<()> {
        self.api
            .submit::<T>(object, event_id, event.clone())
            .await?;
        self.db
            .submit::<T, _>(object, event_id, event, &*self.db)
            .await
    }

    pub async fn get<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<FullObject> {
        match self.db.get::<T>(lock, object_id).await {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let res = self.api.get::<T>(object_id).await?;
        self.db
            .create_all::<T, _>(res.clone(), lock, &*self.db)
            .await?;
        Ok(res)
    }

    pub async fn query_local<'a, T: Object>(
        &'a self,
        q: &'a Query,
    ) -> crate::Result<impl 'a + CrdbStream<Item = crate::Result<FullObject>>> {
        // User is ignored for LocalDb queries
        self.db.query::<T>(User::from_u128(0), None, q).await
    }

    pub async fn query_remote<T: Object>(
        &self,
        lock: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<FullObject>>> {
        Ok(self
            .api
            .query::<T>(ignore_not_modified_on_server_since, q)
            .await?
            .then({
                let db = self.db.clone();
                move |o| {
                    let db = db.clone();
                    async move {
                        let o = o?;
                        db.create_all::<T, _>(o.clone(), lock, &*db).await?;
                        Ok(o)
                    }
                }
            }))
    }

    pub async fn recreate<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
    ) -> crate::Result<()> {
        self.db.recreate::<T, _>(time, object, &*self.db).await?;
        self.api.recreate::<T>(time, object).await
    }

    pub async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        self.db.create_binary(binary_id, data.clone()).await?;
        self.api.create_binary(binary_id, data).await
    }

    pub async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.db.get_binary(binary_id).await? {
            return Ok(Some(res));
        }
        let Some(res) = self.api.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.db.create_binary(binary_id, res.clone()).await?;
        Ok(Some(res))
    }
}

pub struct ClientVacuumSchedule<F> {
    frequency: Duration,
    filter: F,
}

pub struct ClientStorageInfo {
    pub usage: usize,
    pub quota: usize,
    pub objects_unlocked_this_run: usize,
}

impl<F> ClientVacuumSchedule<F> {
    pub fn new(frequency: Duration) -> ClientVacuumSchedule<fn(ClientStorageInfo) -> bool> {
        ClientVacuumSchedule {
            frequency,
            filter: |_| true,
        }
    }

    pub fn never() -> ClientVacuumSchedule<fn(ClientStorageInfo) -> bool> {
        ClientVacuumSchedule {
            frequency: Duration::from_secs(86400),
            filter: |_| false,
        }
    }

    /// If the provided function returns `true`, then vacuum will happen
    ///
    /// The `ClientStorageInfo` provided is all approximate information.
    pub fn filter<G: Fn(ClientStorageInfo) -> bool>(self, filter: G) -> ClientVacuumSchedule<G> {
        ClientVacuumSchedule {
            frequency: self.frequency,
            filter,
        }
    }
}
