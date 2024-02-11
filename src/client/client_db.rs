use super::{connection::ConnectionEvent, ApiDb, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    crdb_internal::Lock,
    db_trait::Db,
    error::ResultExt,
    ids::QueryId,
    messages::{MaybeObject, ObjectData, Update, UpdateData, Updates},
    BinPtr, CrdbStream, EventId, Importance, Object, ObjectId, Query, SessionToken, TypeId,
    Updatedness,
};
use anyhow::anyhow;
use futures::{channel::mpsc, stream, FutureExt, StreamExt};
use std::{
    collections::{hash_map, HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

enum UpdateResult {
    LostAccess,
    LatestUnchanged,
    LatestChanged(serde_json::Value),
}

pub struct ClientDb {
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    subscribed_objects: Arc<Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>)>>>,
    subscribed_queries:
        Arc<Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>>>,
    data_saver: mpsc::UnboundedSender<(
        Arc<Update>,
        Option<Updatedness>,
        Lock,
        oneshot::Sender<crate::Result<UpdateResult>>,
    )>,
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
        let (updates_broadcaster, updates_broadcastee) = broadcast::channel(64);
        let db_bypass = Arc::new(LocalDb::connect(local_db).await?);
        let db = Arc::new(CacheDb::new(db_bypass.clone(), cache_watermark));
        let subscribed_queries = db_bypass
            .get_subscribed_queries()
            .await
            .wrap_context("listing subscribed queries")?;
        let subscribed_objects = db_bypass
            .get_subscribed_objects()
            .await
            .wrap_context("listing subscribed objects")?
            .into_iter()
            .map(|(id, (type_id, value, updatedness))| {
                let queries = Self::queries_for(&subscribed_queries, type_id, &value);
                (id, (updatedness, queries))
            })
            .collect::<HashMap<_, _>>();
        let subscribed_queries = Arc::new(Mutex::new(subscribed_queries));
        let subscribed_objects = Arc::new(Mutex::new(subscribed_objects));
        let (api, updates_receiver) = ApiDb::new(
            {
                let subscribed_objects = subscribed_objects.clone();
                move || {
                    subscribed_objects
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(id, (upd, _))| (*id, *upd))
                        .collect()
                }
            },
            {
                let subscribed_queries = subscribed_queries.clone();
                move || subscribed_queries.lock().unwrap().clone()
            },
            db.clone(),
        );
        let api = Arc::new(api);
        let cancellation_token = CancellationToken::new();
        let (data_saver, data_saver_receiver) = mpsc::unbounded();
        // TODO(api): ObjectDoesNotExist as response to the startup GetSubscribe means that our user lost read access while offline; handle it properly
        // TODO(client): reencode all snapshots to latest version (FTS normalizer & snapshot_version) upon bootup
        let this = ClientDb {
            api,
            db,
            db_bypass,
            subscribed_objects,
            subscribed_queries,
            data_saver,
            updates_broadcastee,
            vacuum_guard: Arc::new(RwLock::new(())),
            _cleanup_token: cancellation_token.clone().drop_guard(),
        };
        this.setup_update_watcher(updates_receiver);
        this.setup_autovacuum(vacuum_schedule, cancellation_token);
        this.setup_data_saver::<C>(data_saver_receiver, updates_broadcaster);
        Ok(this)
    }

    pub fn listen_for_updates(&self) -> broadcast::Receiver<ObjectId> {
        self.updates_broadcastee.resubscribe()
    }

    fn queries_for(
        subscribed_queries: &HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
        type_id: TypeId,
        value: &serde_json::Value,
    ) -> HashSet<QueryId> {
        subscribed_queries
            .iter()
            .filter(|(_, (query, q_type_id, _, _))| {
                type_id == *q_type_id && query.matches_json(&value)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    fn setup_update_watcher(&self, mut updates_receiver: mpsc::UnboundedReceiver<Updates>) {
        // No need for a cancellation token: this task will automatically end as soon as the stream
        // coming from `ApiDb` closes, which will happen when `ApiDb` gets dropped.
        crate::spawn({
            let data_saver = self.data_saver.clone();
            async move {
                while let Some(updates) = updates_receiver.next().await {
                    // TODO(api): How to deal with the case where a query is Importance::Lock, new objects are locked, and then the objects
                    // stop matching the query? In particular, should the objects be unlocked? This will probably require adding a new field
                    // to the database, remembering whether the object was locked for a query or by explicit request from the user, so that
                    // the two could be (un)locked independently, and the object vacuumed-out iff they're both unlocked.
                    // Solution idea: have is_locked be Some(1) if object is locked for itself, Some(2) if locked for a query, and Some(3) if
                    // both
                    for u in updates.data {
                        let (sender, _) = oneshot::channel();
                        data_saver
                            .unbounded_send((
                                u,
                                Some(updates.now_have_all_until),
                                Lock::NONE,
                                sender,
                            ))
                            .expect("data saver thread cannot go away");
                        // Ignore the result of saving
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

    fn setup_data_saver<C: ApiConfig>(
        &self,
        data_receiver: mpsc::UnboundedReceiver<(
            Arc<Update>,
            Option<Updatedness>,
            Lock,
            oneshot::Sender<crate::Result<UpdateResult>>,
        )>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        let db_bypass = self.db_bypass.clone();
        let api = self.api.clone();
        let subscribed_objects = self.subscribed_objects.clone();
        let subscribed_queries = self.subscribed_queries.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        crate::spawn(async move {
            Self::data_saver::<C>(
                data_receiver,
                subscribed_objects,
                subscribed_queries,
                vacuum_guard,
                db_bypass,
                api,
                updates_broadcaster,
            )
            .await
        });
    }

    async fn data_saver<C: ApiConfig>(
        mut data_receiver: mpsc::UnboundedReceiver<(
            Arc<Update>,
            Option<Updatedness>,
            Lock,
            oneshot::Sender<crate::Result<UpdateResult>>,
        )>,
        subscribed_objects: Arc<Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>)>>>,
        subscribed_queries: Arc<
            Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>>,
        >,
        vacuum_guard: Arc<RwLock<()>>,
        db: Arc<LocalDb>,
        api: Arc<ApiDb>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        // Handle all updates in-order! Without that, the updatedness checks will get completely borken up
        while let Some((data, now_have_all_until, lock, result)) = data_receiver.next().await {
            let _guard = vacuum_guard.read().await; // Do not vacuum while we're inserting new data
            match Self::save_data::<C>(
                &db,
                &subscribed_objects,
                &subscribed_queries,
                &data,
                now_have_all_until,
                lock,
                &updates_broadcaster,
            )
            .await
            {
                Ok(res) => {
                    let _ = result.send(Ok(res));
                }
                Err(crate::Error::ObjectDoesNotExist(_)) => {
                    // Ignore this error, because if we received from the server an event with an object that does not exist
                    // locally, it probably means that we recently unsubscribed from it but the server had already sent us the
                    // update.
                }
                Err(crate::Error::MissingBinaries(binary_ids)) => {
                    if let Err(err) = Self::fetch_binaries(binary_ids, &db, &api).await {
                        tracing::error!(
                            ?err,
                            ?data,
                            "failed retrieving required binaries for data the server sent us"
                        );
                        continue;
                    }
                    match Self::save_data::<C>(
                        &db,
                        &subscribed_objects,
                        &subscribed_queries,
                        &data,
                        now_have_all_until,
                        lock,
                        &updates_broadcaster,
                    )
                    .await
                    {
                        Ok(res) => {
                            let _ = result.send(Ok(res));
                        }
                        Err(err) => {
                            tracing::error!(
                                ?err,
                                ?data,
                                "unexpected error saving data after fetching the binaries"
                            );
                            let _ = result.send(Err(err));
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(?err, ?data, "unexpected error saving data");
                }
            }
        }
    }

    async fn fetch_binaries(
        binary_ids: Vec<BinPtr>,
        db: &LocalDb,
        api: &ApiDb,
    ) -> crate::Result<()> {
        let mut bins = stream::iter(binary_ids.into_iter())
            .map(|binary_id| api.get_binary(binary_id).map(move |bin| (binary_id, bin)))
            .buffer_unordered(16); // TODO(low): is 16 a good number?
        while let Some((binary_id, bin)) = bins.next().await {
            match bin? {
                Some(bin) => db.create_binary(binary_id, bin).await?,
                None => {
                    return Err(crate::Error::Other(anyhow!("Binary {binary_id:?} was not present on server, yet server sent us data requiring it")));
                }
            }
        }
        Ok(())
    }

    async fn save_data<C: ApiConfig>(
        db: &LocalDb,
        subscribed_objects: &Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>)>>,
        subscribed_queries: &Mutex<
            HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
        >,
        u: &Update,
        now_have_all_until: Option<Updatedness>,
        lock: Lock,
        updates_broadcaster: &broadcast::Sender<ObjectId>,
    ) -> crate::Result<UpdateResult> {
        let object_id = u.object_id;
        let type_id = u.type_id;
        let res = match &u.data {
            UpdateData::Creation {
                created_at,
                snapshot_version,
                data,
            } => match C::recreate(
                db,
                type_id,
                object_id,
                *created_at,
                *snapshot_version,
                &data,
                now_have_all_until,
                lock,
            )
            .await?
            {
                Some(res) => UpdateResult::LatestChanged(res),
                None => UpdateResult::LatestUnchanged,
            },
            UpdateData::Event { event_id, data } => match C::submit(
                &*db,
                type_id,
                object_id,
                *event_id,
                &data,
                now_have_all_until,
                lock,
            )
            .await?
            {
                Some(res) => UpdateResult::LatestChanged(res),
                None => UpdateResult::LatestUnchanged,
            },
            UpdateData::LostReadRights => {
                subscribed_objects.lock().unwrap().remove(&object_id);
                if let Err(err) = db.remove(object_id).await {
                    tracing::error!(
                        ?err,
                        ?object_id,
                        "failed removing object for which we lost read rights"
                    );
                }
                UpdateResult::LostAccess
            }
        };
        match &res {
            UpdateResult::LostAccess => {
                // Lost access to the object
                let prev = subscribed_objects.lock().unwrap().remove(&object_id);
                if let Some((_, queries)) = prev {
                    if let Some(now_have_all_until) = now_have_all_until {
                        if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                            tracing::error!(
                                ?err,
                                ?queries,
                                "failed updating now_have_all_until for queries"
                            );
                        }
                        let mut subscribed_queries = subscribed_queries.lock().unwrap();
                        for q in queries {
                            if let Some(query) = subscribed_queries.get_mut(&q) {
                                query.2 = Some(now_have_all_until);
                            }
                        }
                    }
                }
            }
            UpdateResult::LatestUnchanged => {
                // No change in the object's latest_snapshot
                if let Some(now_have_all_until) = now_have_all_until {
                    let queries = {
                        let mut subscribed_objects = subscribed_objects.lock().unwrap();
                        let Some(entry) = subscribed_objects.get_mut(&object_id) else {
                            return Err(crate::Error::Other(anyhow!("no change in object for which we received an object, but we were not subscribed to it yet?")));
                        };
                        entry.0 = std::cmp::max(entry.0, Some(now_have_all_until));
                        entry.1.clone()
                    };
                    if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                        tracing::error!(
                            ?err,
                            ?queries,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                    let mut subscribed_queries = subscribed_queries.lock().unwrap();
                    for q in queries {
                        if let Some(query) = subscribed_queries.get_mut(&q) {
                            query.2 = std::cmp::max(query.2, Some(now_have_all_until));
                        }
                    }
                }
            }
            UpdateResult::LatestChanged(res) => {
                // Something changed in the object's latest_snapshot
                if let Some(now_have_all_until) = now_have_all_until {
                    let mut queries =
                        Self::queries_for(&subscribed_queries.lock().unwrap(), type_id, res);
                    if let Some((_, queries_before)) = subscribed_objects
                        .lock()
                        .unwrap()
                        .insert(object_id, (Some(now_have_all_until), queries.clone()))
                    {
                        queries.extend(queries_before);
                    }
                    if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                        tracing::error!(
                            ?err,
                            ?queries,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                    let mut subscribed_queries = subscribed_queries.lock().unwrap();
                    for q in queries {
                        if let Some(query) = subscribed_queries.get_mut(&q) {
                            query.2 = Some(now_have_all_until);
                        }
                    }
                }
            }
        }
        // TODO(client): give out update broadcasters for each individual query the user subscribed to
        if let Err(err) = updates_broadcaster.send(object_id) {
            tracing::error!(?err, ?object_id, "failed broadcasting update");
        }
        Ok(res)
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
        self.db.unlock(Lock::OBJECT, ptr).await
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

    pub async fn unsubscribe_query(&self, query_id: QueryId) -> crate::Result<()> {
        self.subscribed_queries.lock().unwrap().remove(&query_id);
        self.api.unsubscribe_query(query_id);
        self.db_bypass.unsubscribe_query(query_id).await
    }

    pub async fn create<T: Object>(
        &self,
        importance: Importance,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        // TODO(client): validate permissions to create the object, to fail early
        let val = self
            .db
            .create(
                object_id,
                created_at,
                object.clone(),
                None, // Locally-created object, has no updatedness yet
                importance.to_object_lock(),
            )
            .await?;
        if importance >= Importance::Subscribe {
            if let Some(val) = val {
                let val_json =
                    serde_json::to_value(val).wrap_context("serializing new last snapshot")?;
                let queries = Self::queries_for(
                    &self.subscribed_queries.lock().unwrap(),
                    *T::type_ulid(),
                    &val_json,
                );
                self.subscribed_objects
                    .lock()
                    .unwrap()
                    .insert(object_id, (None, queries));
            }
        }
        self.api.create(
            object_id,
            created_at,
            object,
            importance >= Importance::Subscribe,
        )
    }

    pub async fn submit<T: Object>(
        &self,
        importance: Importance,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        // TODO(client): validate permissions to submit the event, to fail early
        self.db
            .submit::<T>(
                object,
                event_id,
                event.clone(),
                None, // Locally-submitted event, has no updatedness yet
                importance.to_object_lock(),
            )
            .await?;
        // The object must already be subscribed, in order for event submission to be successful.
        // So, there is no need to manually subscribe again.
        // TODO(low): consider introducing a ManuallyUpdated importance level, that would make this statement wrong?
        self.api
            .submit::<T>(object, event_id, event, importance >= Importance::Subscribe)
    }

    /// Returns the latest snapshot for the object described by `data`
    async fn locally_create_all<T: Object>(
        data_saver: &mpsc::UnboundedSender<(
            Arc<Update>,
            Option<Updatedness>,
            Lock,
            oneshot::Sender<crate::Result<UpdateResult>>,
        )>,
        db: &CacheDb<LocalDb>,
        lock: Lock,
        data: ObjectData,
    ) -> crate::Result<Option<serde_json::Value>> {
        if data.type_id != *T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id: data.object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: data.type_id,
            });
        }

        // First, submit object creation request
        let mut latest_snapshot_returns =
            Vec::with_capacity(data.events.len() + data.creation_snapshot.is_some() as usize);
        if let Some((created_at, snapshot_version, snapshot_data)) = data.creation_snapshot {
            let (sender, receiver) = oneshot::channel();
            latest_snapshot_returns.push(receiver);
            data_saver
                .unbounded_send((
                    Arc::new(Update {
                        object_id: data.object_id,
                        type_id: data.type_id,
                        data: UpdateData::Creation {
                            created_at,
                            snapshot_version,
                            data: snapshot_data,
                        },
                    }),
                    data.events.is_empty().then(|| data.now_have_all_until),
                    lock,
                    sender,
                ))
                .map_err(|_| crate::Error::Other(anyhow!("Data saver task disappeared early")))?;
        } else if lock != Lock::NONE {
            db.get_latest::<T>(lock, data.object_id)
                .await
                .wrap_context("locking object as requested")?;
        }

        // Then, submit all the events
        let events_len = data.events.len();
        for (i, (event_id, event)) in data.events.into_iter().enumerate() {
            // We already locked the object just above if requested
            let (sender, receiver) = oneshot::channel();
            latest_snapshot_returns.push(receiver);
            data_saver
                .unbounded_send((
                    Arc::new(Update {
                        object_id: data.object_id,
                        type_id: data.type_id,
                        data: UpdateData::Event {
                            event_id,
                            data: event,
                        },
                    }),
                    (i + 1 == events_len).then(|| data.now_have_all_until),
                    Lock::NONE,
                    sender,
                ))
                .map_err(|_| crate::Error::Other(anyhow!("Data saver task disappeared early")))?;
        }

        // Finally, retrieve the result
        let mut res = None;
        for receiver in latest_snapshot_returns {
            if let Ok(res_data) = receiver.await {
                match res_data {
                    Ok(UpdateResult::LatestChanged(res_data)) => res = Some(res_data),
                    Ok(_) => (),
                    Err(crate::Error::ObjectDoesNotExist(o))
                        if o == data.object_id && lock == Lock::NONE =>
                    {
                        // Ignore this error, the object was just vacuumed during creation
                    }
                    Err(err) => return Err(err).wrap_context("creating in local database"),
                }
            }
        }

        Ok(res)
    }

    async fn get_impl<T: Object>(
        &self,
        lock: Lock,
        subscribe: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        match self.db.get_latest::<T>(lock, object_id).await {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        if subscribe {
            let res = self.api.get_subscribe(object_id).await?;
            let res_json =
                Self::locally_create_all::<T>(&self.data_saver, &self.db, lock, res).await?;
            let (res, res_json) = match res_json {
                Some(res_json) => (
                    Arc::new(T::deserialize(&res_json).wrap_context("deserializing snapshot")?),
                    res_json,
                ),
                None => {
                    let res = self.db.get_latest::<T>(Lock::NONE, object_id).await?;
                    let res_json =
                        serde_json::to_value(&res).wrap_context("serializing snapshot")?;
                    (res, res_json)
                }
            };
            let queries = Self::queries_for(
                &self.subscribed_queries.lock().unwrap(),
                *T::type_ulid(),
                &res_json,
            );
            self.subscribed_objects
                .lock()
                .unwrap()
                .insert(object_id, (None, queries));
            Ok(res)
        } else {
            unimplemented!() // TODO(client): GetLatest
        }
    }

    pub async fn get<T: Object>(
        &self,
        importance: Importance,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        self.get_impl(
            importance.to_object_lock(),
            importance.to_subscribe(),
            object_id,
        )
        .await
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
                match self.get_impl::<T>(importance.to_query_lock(), importance.to_subscribe(), object_id).await {
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
        importance: Importance,
        query_id: QueryId,
        query: Arc<Query>,
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<Arc<T>>>> {
        if importance >= Importance::Subscribe {
            // TODO(client): first make sure to wait until the current upload queue is empty, so that
            // any newly-created object/event makes its way through to the server before querying
            let only_updated_since = {
                let mut subscribed_queries = self.subscribed_queries.lock().unwrap();
                let entry = subscribed_queries.entry(query_id);
                match entry {
                    hash_map::Entry::Occupied(mut o) => {
                        if !o.get().3.contains(Lock::FOR_QUERIES) && importance >= Importance::Lock
                        {
                            // Increasing the lock behavior. Re-subscribe from scratch
                            o.get_mut().3 |= Lock::FOR_QUERIES;
                            o.get_mut().2 = None; // Force have_all_until to 0 as previous stuff could have been vacuumed
                            None
                        } else {
                            // The query was already locked enough. Just proceed.
                            o.get().2
                        }
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert((
                            query.clone(),
                            *T::type_ulid(),
                            None,
                            importance.to_query_lock(),
                        ));
                        None
                    }
                }
            };
            if only_updated_since.is_none() {
                // Was not subscribed yet
                self.db_bypass
                    .subscribe_query(query_id, query.clone(), importance >= Importance::Lock)
                    .await?;
            }
            Ok(self
                .api
                .query_subscribe::<T>(query_id, only_updated_since, query)
                .then({
                    let data_saver = self.data_saver.clone();
                    let db = self.db.clone();
                    let subscribed_objects = self.subscribed_objects.clone();
                    let subscribed_queries = self.subscribed_queries.clone();
                    let lock = importance.to_query_lock();
                    move |data| {
                        let data_saver = data_saver.clone();
                        let db = db.clone();
                        let subscribed_objects = subscribed_objects.clone();
                        let subscribed_queries = subscribed_queries.clone();
                        async move {
                            let (data, updatedness) = data?;
                            match data {
                                MaybeObject::NotYetSubscribed(data) => {
                                    let now_have_all_until = data.now_have_all_until;
                                    let object_id = data.object_id;
                                    let type_id = data.type_id;
                                    let res_json =
                                        Self::locally_create_all::<T>(&data_saver, &db, lock, data)
                                            .await?;
                                    let (res, res_json) = match res_json {
                                        Some(res_json) => (
                                            Arc::new(
                                                T::deserialize(&res_json)
                                                    .wrap_context("deserializing snapshot")?,
                                            ),
                                            res_json,
                                        ),
                                        None => {
                                            let res = self
                                                .db
                                                .get_latest::<T>(Lock::NONE, object_id)
                                                .await?;
                                            let res_json = serde_json::to_value(&res)
                                                .wrap_context("serializing snapshot")?;
                                            (res, res_json)
                                        }
                                    };
                                    let queries = Self::queries_for(
                                        &subscribed_queries.lock().unwrap(),
                                        type_id,
                                        &res_json,
                                    );
                                    if let Some(now_have_all_until) = updatedness {
                                        db.update_queries(&queries, now_have_all_until).await?;
                                    }
                                    subscribed_objects
                                        .lock()
                                        .unwrap()
                                        .insert(object_id, (Some(now_have_all_until), queries));
                                    if let Some(q) =
                                        subscribed_queries.lock().unwrap().get_mut(&query_id)
                                    {
                                        q.2 = updatedness.or(q.2);
                                    }
                                    Ok(res)
                                }
                                MaybeObject::AlreadySubscribed(object_id) => {
                                    self.db.get_latest::<T>(lock, object_id).await
                                }
                            }
                        }
                    }
                }))
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
