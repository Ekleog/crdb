use super::{connection::ConnectionEvent, ApiDb, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::Db,
    full_object::FullObject,
    messages::{Update, UpdateData},
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp, User,
};
use futures::{channel::mpsc, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

pub struct ClientDb {
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    updates_broadcastee: broadcast::Receiver<ObjectId>,
    vacuum_guard: Arc<RwLock<()>>,
    _cleanup_token: tokio_util::sync::DropGuard,
}

impl ClientDb {
    pub async fn new<C: ApiConfig, F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        local_db: &str,
        cache_watermark: usize,
        vacuum_schedule: ClientVacuumSchedule<F>,
    ) -> anyhow::Result<ClientDb> {
        C::check_ulids();
        let (api, updates_receiver) = ApiDb::new();
        let (updates_broadcaster, updates_broadcastee) = broadcast::channel(64);
        let api = Arc::new(api);
        let db_bypass = Arc::new(LocalDb::connect(local_db).await?);
        let db = CacheDb::new(db_bypass.clone(), cache_watermark);
        let cancellation_token = CancellationToken::new();
        let this = ClientDb {
            api,
            db,
            db_bypass,
            updates_broadcastee,
            vacuum_guard: Arc::new(RwLock::new(())),
            _cleanup_token: cancellation_token.clone().drop_guard(),
        };
        this.setup_watchers::<C>(updates_receiver, updates_broadcaster);
        this.setup_autovacuum(vacuum_schedule, cancellation_token);
        Ok(this)
    }

    pub fn listen_for_updates(&self) -> broadcast::Receiver<ObjectId> {
        self.updates_broadcastee.resubscribe()
    }

    fn setup_watchers<C: ApiConfig>(
        &self,
        mut updates_receiver: mpsc::UnboundedReceiver<Update>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        // No need for a cancellation token: this task will automatically end as soon as the stream
        // coming from `ApiDb` closes, which will happen when `ApiDb` gets dropped.
        crate::spawn({
            let local_db = self.db.clone();
            async move {
                while let Some(u) = updates_receiver.next().await {
                    let object_id = u.object_id;
                    let type_id = u.type_id;
                    // TODO(client): record `u.now_have_all_until` somewhere
                    match u.data {
                        UpdateData::Creation { created_at, data } => {
                            if let Err(err) =
                                C::create(&*local_db, type_id, object_id, created_at, data, false)
                                    .await
                            {
                                tracing::error!(
                                    ?err,
                                    ?object_id,
                                    "failed creating received object in internal db"
                                );
                            }
                        }
                        UpdateData::Event { event_id, data } => {
                            if let Err(err) =
                                C::submit(&*local_db, type_id, object_id, event_id, data).await
                            {
                                tracing::error!(
                                    ?err,
                                    ?object_id,
                                    ?event_id,
                                    "failed submitting received object to internal db"
                                );
                            }
                            // DO NOT re-fetch object when receiving an event not in cache for it.
                            // Without this, users would risk unsubscribing from an object, then receiving
                            // an event on this object (as a race condition), and then staying subscribed.
                        }
                        UpdateData::Recreation { time } => {
                            if let Err(err) =
                                C::recreate(&*local_db, type_id, object_id, time).await
                            {
                                tracing::error!(
                                    ?err,
                                    ?object_id,
                                    "failed recreating as per received event in internal db"
                                );
                            }
                        }
                    }
                    if let Err(err) = updates_broadcaster.send(object_id) {
                        tracing::error!(?err, ?object_id, "failed broadcasting update");
                    }
                }
            }
        });
    }

    fn setup_autovacuum<F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        &self,
        vacuum_schedule: ClientVacuumSchedule<F>,
        cancellation_token: CancellationToken,
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

                tokio::select! {
                    _ = tokio::time::sleep(vacuum_schedule.frequency) => (),
                    _ = cancellation_token.cancelled() => break,
                }
            }
        });
    }

    /// `cb` will be called with the parameter `true` if we just connected (again), and `false` if
    /// we just noticed a disconnection.
    pub fn on_connection_event(&self, cb: impl 'static + Send + Sync + Fn(ConnectionEvent)) {
        self.api.on_connection_event(cb)
    }

    pub fn login(&self, url: Arc<String>, token: SessionToken) {
        self.api.login(url, token)
    }

    pub async fn logout(&self) -> anyhow::Result<()> {
        self.api.logout();
        // TODO(api): flush self.db
        Ok(())
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

    pub async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        self.db.create_binary(binary_id, data.clone()).await?;
        self.api.create_binary(binary_id, data).await
    }

    pub async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
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
