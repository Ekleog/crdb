use super::{connection::ConnectionEvent, ApiDb, LocalDb, ShouldLock};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::Db,
    error::ResultExt,
    ids::QueryId,
    messages::{MaybeObject, ObjectData, Update, UpdateData, Updates},
    object::parse_snapshot_ref,
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
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

pub struct ClientDb {
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    subscribed_objects: Arc<Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>)>>>,
    subscribed_queries:
        Arc<Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, ShouldLock)>>>,
    data_saver: mpsc::UnboundedSender<(Arc<Update>, Option<Updatedness>)>,
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
        subscribed_queries: &HashMap<
            QueryId,
            (Arc<Query>, TypeId, Option<Updatedness>, ShouldLock),
        >,
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
                    for u in updates.data {
                        data_saver
                            .unbounded_send((u, Some(updates.now_have_all_until)))
                            .expect("data saver thread cannot go away");
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
        data_receiver: mpsc::UnboundedReceiver<(Arc<Update>, Option<Updatedness>)>,
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
        mut data_receiver: mpsc::UnboundedReceiver<(Arc<Update>, Option<Updatedness>)>,
        subscribed_objects: Arc<Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>)>>>,
        subscribed_queries: Arc<
            Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, ShouldLock)>>,
        >,
        vacuum_guard: Arc<RwLock<()>>,
        db: Arc<LocalDb>,
        api: Arc<ApiDb>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        // Handle all updates in-order! Without that, the updatedness checks will get completely borken up
        while let Some((data, now_have_all_until)) = data_receiver.next().await {
            let _guard = vacuum_guard.read().await; // Do not vacuum while we're inserting new data
            match Self::save_data::<C>(
                &db,
                &subscribed_objects,
                &subscribed_queries,
                &data,
                now_have_all_until,
                &updates_broadcaster,
            )
            .await
            {
                Ok(()) => (),
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
                    if let Err(err) = Self::save_data::<C>(
                        &db,
                        &subscribed_objects,
                        &subscribed_queries,
                        &data,
                        now_have_all_until,
                        &updates_broadcaster,
                    )
                    .await
                    {
                        tracing::error!(
                            ?err,
                            ?data,
                            "unexpected error saving data after fetching the binaries"
                        );
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
            HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, ShouldLock)>,
        >,
        u: &Update,
        now_have_all_until: Option<Updatedness>,
        updates_broadcaster: &broadcast::Sender<ObjectId>,
    ) -> crate::Result<()> {
        let object_id = u.object_id;
        let type_id = u.type_id;
        let res = match &u.data {
            UpdateData::Creation {
                created_at,
                snapshot_version,
                data,
            } => Some(
                C::recreate(
                    db,
                    type_id,
                    object_id,
                    *created_at,
                    *snapshot_version,
                    &data,
                    now_have_all_until,
                    false,
                )
                .await?,
            ),
            UpdateData::Event { event_id, data } => Some(
                C::submit(
                    &*db,
                    type_id,
                    object_id,
                    *event_id,
                    &data,
                    now_have_all_until,
                    false,
                )
                .await?,
            ),
            UpdateData::LostReadRights => {
                subscribed_objects.lock().unwrap().remove(&object_id);
                if let Err(err) = db.remove(object_id).await {
                    tracing::error!(
                        ?err,
                        ?object_id,
                        "failed removing object for which we lost read rights"
                    );
                }
                None
            }
        };
        match res {
            None => {
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
            Some(None) => {
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
            Some(Some(res)) => {
                // Something changed in the object's latest_snapshot
                if let Some(now_have_all_until) = now_have_all_until {
                    let mut queries =
                        Self::queries_for(&subscribed_queries.lock().unwrap(), type_id, &res);
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
        Ok(())
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
                importance >= Importance::Lock,
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
                importance >= Importance::Lock,
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
        db: &CacheDb<LocalDb>,
        lock: bool,
        data: ObjectData,
    ) -> crate::Result<Option<Arc<T>>> {
        // TODO(api): this should reuse the data_saver logic
        if data.type_id != *T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id: data.object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: data.type_id,
            });
        }

        let mut res = None;
        if let Some((created_at, snapshot_version, snapshot_data)) = data.creation_snapshot {
            let creation_snapshot = parse_snapshot_ref::<T>(snapshot_version, &snapshot_data)
                .wrap_context("parsing snapshot")?;

            res = db
                .create::<T>(
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
                Ok(r) => res = r.or(res),
                Err(crate::Error::ObjectDoesNotExist(object_id)) // Vacuumed in-between
                    if object_id == data.object_id && !lock => (),
                Err(err) => return Err(err).wrap_context("creating event in local database"),
            }
        }

        Ok(res)
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
            let res = self.api.get_subscribe(object_id).await?;
            let res = Self::locally_create_all::<T>(&self.db, importance >= Importance::Lock, res)
                .await?;
            let res = match res {
                Some(res) => res,
                None => self.db.get_latest::<T>(false, object_id).await?,
            };
            let queries = Self::queries_for(
                &self.subscribed_queries.lock().unwrap(),
                *T::type_ulid(),
                &serde_json::to_value(&res).wrap_context("serializing latest snapshot")?,
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
                        if !o.get().3 .0 && importance >= Importance::Lock {
                            // Increasing the lock behavior. Re-subscribe from scratch
                            o.insert((query.clone(), *T::type_ulid(), None, importance.into()));
                            None
                        } else {
                            // The query was already locked enough. Just proceed.
                            o.get().2
                        }
                    }
                    hash_map::Entry::Vacant(v) => {
                        v.insert((query.clone(), *T::type_ulid(), None, importance.into()));
                        None
                    }
                }
            };
            if only_updated_since.is_none() {
                // Was not subscribed yet
                self.db_bypass
                    .subscribe_query(query_id, query.clone(), importance.into())
                    .await?;
            }
            Ok(self
                .api
                .query_subscribe::<T>(query_id, only_updated_since, query)
                .then({
                    let db = self.db.clone();
                    let subscribed_objects = self.subscribed_objects.clone();
                    let subscribed_queries = self.subscribed_queries.clone();
                    let lock = importance >= Importance::Lock;
                    move |data| {
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
                                    let res = match Self::locally_create_all::<T>(&db, lock, data)
                                        .await?
                                    {
                                        Some(res) => res,
                                        None => db.get_latest::<T>(false, object_id).await?,
                                    };
                                    let res_json = serde_json::to_value(&res)
                                        .wrap_context("serializing latest snapshot")?;
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
