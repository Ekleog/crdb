use super::{connection::ConnectionEvent, ApiDb, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::Db,
    error::ResultExt,
    ids::QueryId,
    messages::{MaybeObject, ObjectData, UpdateData, Updates},
    object::parse_snapshot_ref,
    BinPtr, CrdbStream, EventId, Importance, Object, ObjectId, Query, SessionToken, Updatedness,
};
use futures::{channel::mpsc, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

pub struct ClientDb {
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    subscribed_objects: Arc<Mutex<HashMap<ObjectId, Option<Updatedness>>>>,
    error_sender: mpsc::UnboundedSender<crate::Error>,
    updates_broadcastee: broadcast::Receiver<ObjectId>,
    vacuum_guard: Arc<RwLock<()>>,
    _cleanup_token: tokio_util::sync::DropGuard,
}

impl ClientDb {
    pub async fn new<C: ApiConfig, F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        local_db: &str,
        cache_watermark: usize,
        vacuum_schedule: ClientVacuumSchedule<F>,
    ) -> anyhow::Result<(ClientDb, mpsc::UnboundedReceiver<crate::Error>)> {
        C::check_ulids();
        let (error_sender, error_receiver) = mpsc::unbounded();
        let (updates_broadcaster, updates_broadcastee) = broadcast::channel(64);
        let db_bypass = Arc::new(LocalDb::connect(local_db).await?);
        let db = Arc::new(CacheDb::new(db_bypass.clone(), cache_watermark));
        let subscribed_objects = Arc::new(Mutex::new(
            db_bypass
                .get_subscribed_objects()
                .await
                .wrap_context("listing subscribed objects")?,
        ));
        let (api, updates_receiver) = ApiDb::new(
            {
                let subscribed_objects = subscribed_objects.clone();
                move || subscribed_objects.lock().unwrap().clone()
            },
            || std::iter::empty(), // TODO(api)
        );
        let api = Arc::new(api);
        let cancellation_token = CancellationToken::new();
        // TODO(api): ObjectDoesNotExist as response to the startup GetSubscribe means that our user lost read access while offline; handle it properly
        // TODO(client): reencode all snapshots to latest version (FTS normalizer & snapshot_version) upon bootup
        let this = ClientDb {
            api,
            db,
            db_bypass,
            subscribed_objects,
            error_sender,
            updates_broadcastee,
            vacuum_guard: Arc::new(RwLock::new(())),
            _cleanup_token: cancellation_token.clone().drop_guard(),
        };
        this.setup_watchers::<C>(updates_receiver, updates_broadcaster);
        this.setup_autovacuum(vacuum_schedule, cancellation_token);
        Ok((this, error_receiver))
    }

    pub fn listen_for_updates(&self) -> broadcast::Receiver<ObjectId> {
        self.updates_broadcastee.resubscribe()
    }

    fn setup_watchers<C: ApiConfig>(
        &self,
        mut updates_receiver: mpsc::UnboundedReceiver<Updates>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        // No need for a cancellation token: this task will automatically end as soon as the stream
        // coming from `ApiDb` closes, which will happen when `ApiDb` gets dropped.
        crate::spawn({
            let local_db = self.db.clone();
            let subscribed_objects = self.subscribed_objects.clone();
            async move {
                while let Some(updates) = updates_receiver.next().await {
                    for u in updates.data {
                        let object_id = u.object_id;
                        let type_id = u.type_id;
                        // TODO(api): make sure that incoming Updates properly bumps the database-recorded queries' now_have_all_until
                        match &u.data {
                            UpdateData::Creation {
                                created_at,
                                snapshot_version,
                                data,
                            } => {
                                // TODO(client): decide how to expose locking all of a query's results, including new objects
                                // TODO(api): automatically handle MissingBinaries error by requesting them from server and retrying
                                let res = C::recreate(
                                    &*local_db,
                                    type_id,
                                    object_id,
                                    *created_at,
                                    *snapshot_version,
                                    &data,
                                    Some(updates.now_have_all_until),
                                    false,
                                )
                                .await;
                                match res {
                                    Ok(()) => {
                                        subscribed_objects
                                            .lock()
                                            .unwrap()
                                            .insert(object_id, Some(updates.now_have_all_until));
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            ?err,
                                            ?object_id,
                                            "failed creating received object in internal db"
                                        );
                                    }
                                }
                            }
                            UpdateData::Event { event_id, data } => {
                                // TODO(client): decide how to expose locking all of a query's results, including objects that start
                                // matching the query only after the initial run
                                // TODO(api): automatically handle MissingBinaries error by requesting them from server and retrying
                                let res = C::submit(
                                    &*local_db,
                                    type_id,
                                    object_id,
                                    *event_id,
                                    &data,
                                    Some(updates.now_have_all_until),
                                    false,
                                )
                                .await;
                                match res {
                                    Ok(()) => {
                                        subscribed_objects
                                            .lock()
                                            .unwrap()
                                            .insert(object_id, Some(updates.now_have_all_until));
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            ?err,
                                            ?object_id,
                                            "failed submitting received event to internal db"
                                        );
                                    }
                                }
                                // TODO(client): avoid tracing::error! when below comment holds
                                // DO NOT re-fetch object when receiving an event not in cache for it.
                                // Without this, users would risk unsubscribing from an object, then receiving
                                // an event on this object (as a race condition), and then staying subscribed.
                            }
                            UpdateData::LostReadRights => {
                                subscribed_objects.lock().unwrap().remove(&object_id);
                                if let Err(err) = local_db.remove(object_id).await {
                                    tracing::error!(
                                        ?err,
                                        ?object_id,
                                        "failed removing object for which we lost read rights"
                                    );
                                }
                            }
                        }
                        // TODO(client): give out update broadcasters for each individual query the user subscribed to
                        if let Err(err) = updates_broadcaster.send(object_id) {
                            tracing::error!(?err, ?object_id, "failed broadcasting update");
                        }
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
        self.db.remove_everything().await?;
        Ok(())
    }

    pub async fn pause_vacuum(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.vacuum_guard.read().await
    }

    pub async fn unlock(&self, ptr: ObjectId) -> crate::Result<()> {
        self.db.unlock(ptr).await
    }

    pub async fn unsubscribe(&self, object_ids: HashSet<ObjectId>) -> crate::Result<()> {
        // TODO(client): automatically call `api.unsubscribe` when `vacuum` removes an object, and remove from subscribed_objects
        for object_id in object_ids.iter() {
            self.db.remove(*object_id).await?;
            self.subscribed_objects.lock().unwrap().remove(object_id);
        }
        self.api.unsubscribe(object_ids);
        Ok(())
    }

    pub async fn create<T: Object>(
        &self,
        importance: Importance,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<oneshot::Receiver<crate::Result<()>>> {
        // TODO(client): validate permissions to create the object, to fail early
        self.db
            .create(
                object_id,
                created_at,
                object.clone(),
                None, // Locally-created object, has no updatedness yet
                importance >= Importance::Lock,
            )
            .await?;
        if importance >= Importance::Subscribe {
            self.subscribed_objects
                .lock()
                .unwrap()
                .insert(object_id, None);
        }
        self.api.create(
            object_id,
            created_at,
            object,
            importance >= Importance::Subscribe,
            self.db.clone(),
            self.error_sender.clone(),
        )
    }

    pub async fn submit<T: Object>(
        &self,
        importance: Importance,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<oneshot::Receiver<crate::Result<()>>> {
        // TODO(client): validate permissions to submit the event, to fail early
        self.db
            .submit::<T>(
                object,
                event_id,
                event.clone(),
                None, // Locally-submitted event, has no updatedness yet
                importance >= Importance::Lock,
            )
            .await?;
        // The object must already be subscribed, in order for event submission to be successful.
        // So, there is no need to manually subscribe again.
        self.api.submit::<T, _>(
            object,
            event_id,
            event,
            importance >= Importance::Subscribe,
            self.db.clone(),
            self.error_sender.clone(),
        )
    }

    /// Returns the latest snapshot for the object described by `data`
    async fn locally_create_all<T: Object>(
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

        if let Some((created_at, snapshot_version, snapshot_data)) = data.creation_snapshot {
            let creation_snapshot = parse_snapshot_ref::<T>(snapshot_version, &snapshot_data)
                .wrap_context("parsing snapshot")?;

            db.create::<T>(
                data.object_id,
                created_at,
                Arc::new(creation_snapshot),
                Some(data.now_have_all_until),
                lock,
            )
            .await
            .wrap_context("creating creation snapshot in local database")?;
        } else if lock {
            db.get_latest::<T>(true, data.object_id)
                .await
                .wrap_context("locking object as requested")?;
        }

        for (event_id, event) in data.events {
            let event = <T::Event as serde::Deserialize>::deserialize(&*event)
                .wrap_context("parsing event")?;

            // We already locked the object just above if requested
            match db
                .submit::<T>(data.object_id, event_id, Arc::new(event), Some(data.now_have_all_until), false)
                .await
            {
                Ok(_) => (),
                Err(crate::Error::ObjectDoesNotExist(object_id)) // Vacuumed in-between
                    if object_id == data.object_id && !lock => (),
                Err(err) => return Err(err).wrap_context("creating event in local database"),
            }
        }

        Ok(())
    }

    pub async fn get<T: Object>(
        &self,
        importance: Importance,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        match self
            .db
            .get_latest::<T>(importance >= Importance::Lock, object_id)
            .await
        {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        if importance >= Importance::Subscribe {
            self.subscribed_objects
                .lock()
                .unwrap()
                .insert(object_id, None);
            let res = self.api.get_subscribe(object_id).await?;
            Self::locally_create_all::<T>(&self.db, importance >= Importance::Lock, res).await?;
            Ok(self.db.get_latest::<T>(false, object_id).await?) // Already locked just above
        } else {
            unimplemented!() // TODO(client): GetLatest
        }
    }

    pub async fn query_local<T: Object>(
        &self,
        importance: Importance,
        query: Arc<Query>,
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<Arc<T>>>> {
        let object_ids = self
            .db
            .query::<T>(query)
            .await
            .wrap_context("listing objects matching query")?;

        Ok(async_stream::stream! {
            for object_id in object_ids {
                match self.get::<T>(importance, object_id).await {
                    Ok(res) => yield Ok(res),
                    // Ignore missing objects, they were just vacuumed between listing and getting
                    Err(crate::Error::ObjectDoesNotExist(id)) if id == object_id => continue,
                    Err(err) => yield Err(err),
                }
            }
        })
    }

    pub fn query_remote<T: Object>(
        &self,
        importance: Importance,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> impl '_ + CrdbStream<Item = crate::Result<Arc<T>>> {
        if importance >= Importance::Subscribe {
            // TODO(client): first make sure to wait until the current upload queue is empty, so that
            // any newly-created object/event makes its way through to the server before querying
            self.api
                .query_subscribe::<T>(QueryId::now(), only_updated_since, query)
                .then({
                    let db = self.db.clone();
                    let subscribed_objects = self.subscribed_objects.clone();
                    let lock = importance >= Importance::Lock;
                    move |data| {
                        let db = db.clone();
                        let subscribed_objects = subscribed_objects.clone();
                        async move {
                            let object_id = match data? {
                                MaybeObject::NotYetSubscribed(data) => {
                                    let object_id = data.object_id;
                                    subscribed_objects.lock().unwrap().insert(object_id, None);
                                    Self::locally_create_all::<T>(&db, lock, data).await?;
                                    object_id
                                }
                                MaybeObject::AlreadySubscribed(object_id) => object_id,
                            };
                            self.db.get_latest::<T>(lock, object_id).await
                        }
                    }
                })
        } else {
            unimplemented!() // TODO(client): FetchLatest
        }
    }

    // TODO(low): should the client be allowed to request a recreation?

    pub async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        self.db.create_binary(binary_id, data.clone()).await
        // Do not create the binary over the API. We'll try uploading the object that requires it when
        // that happens, hoping for the binary to already be known by the server. If it is not, then we'll
        // create the binary there then re-send the object. This avoids spurious binary retransmits, and
        // needs to be handled anyway because the server could have vacuumed between the create_binary and
        // the create_object.
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
