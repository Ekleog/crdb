use super::{connection::ConnectionEvent, ApiDb, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::Db,
    error::ResultExt,
    messages::{ObjectData, Update, UpdateData},
    object::parse_snapshot,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, SessionToken, Timestamp,
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

    /// Returns the latest snapshot for the object described by `data`
    async fn create_all<T: Object>(
        db: &CacheDb<LocalDb>,
        lock: bool,
        data: ObjectData,
    ) -> crate::Result<()> {
        if data.type_id != *T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id: data.object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: data.type_id,
            });
        }

        if let Some(creation_snapshot) = data.creation_snapshot {
            let creation_snapshot = parse_snapshot::<T>(creation_snapshot.0, creation_snapshot.1)
                .wrap_context("parsing snapshot")?;

            db.create::<T, _>(
                data.object_id,
                data.created_at,
                Arc::new(creation_snapshot),
                lock,
                &*db,
            )
            .await
            .wrap_context("creating creation snapshot in local database")?;
        }

        for (event_id, event) in data.events {
            let event = serde_json::from_value::<T::Event>(event).wrap_context("parsing event")?;

            db.submit::<T, _>(data.object_id, event_id, Arc::new(event), &*db)
                .await
                .wrap_context("creating event in local database")?;
        }

        Ok(())
    }

    pub async fn get<T: Object>(&self, lock: bool, object_id: ObjectId) -> crate::Result<Arc<T>> {
        match self.db.get_latest::<T>(lock, object_id).await {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let res = self.api.get_all(object_id).await?;
        Self::create_all::<T>(&self.db, lock, res).await?;
        Ok(self.db.get_latest::<T>(lock, object_id).await?)
    }

    pub async fn query_local<'a, T: Object>(
        &'a self,
        lock: bool,
        q: &'a Query,
    ) -> crate::Result<impl 'a + CrdbStream<Item = crate::Result<Arc<T>>>> {
        let object_ids = self
            .db
            .query::<T>(q)
            .await
            .wrap_context("listing objects matching query")?;

        Ok(async_stream::stream! {
            for object_id in object_ids {
                match self.get::<T>(lock, object_id).await {
                    Ok(res) => yield Ok(res),
                    // Ignore missing objects, they were just vacuumed between listing and getting
                    Err(crate::Error::ObjectDoesNotExist(id)) if id == object_id => continue,
                    Err(err) => yield Err(err),
                }
            }
        })
    }

    pub async fn query_remote<T: Object>(
        &self,
        lock: bool,
        only_updated_since: Option<Timestamp>,
        query: &Query,
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<Arc<T>>>> {
        Ok(self.api.query::<T>(only_updated_since, query).await?.then({
            let db = self.db.clone();
            move |data| {
                let db = db.clone();
                async move {
                    let data = data?;
                    let object_id = data.object_id;
                    Self::create_all::<T>(&db, lock, data).await?;
                    Ok(self.db.get_latest::<T>(lock, object_id).await?)
                }
            }
        }))
    }

    pub async fn recreate<T: Object>(
        &self,
        object: ObjectId,
        time: Timestamp,
    ) -> crate::Result<()> {
        self.db.recreate::<T, _>(object, time, &*self.db).await?;
        self.api.recreate::<T>(object, time).await
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
