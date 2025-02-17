use super::{api_db::OnError, connection::ConnectionEvent, ApiDb};
use crate::Obj;
use anyhow::anyhow;
use crdb_cache::CacheDb;
use crdb_core::{
    BinPtr, ClientSideDb, Db, DbPtr, EventId, Importance, MaybeObject, Object, ObjectData,
    ObjectId, Query, QueryId, ResultExt, SavedQuery, Session, SessionRef, SessionToken, TypeId,
    Update, UpdateData, Updatedness, Updates, Upload, UploadId, User,
};
use crdb_core::{ClientStorageInfo, LoginInfo};
use futures::{channel::mpsc, stream, FutureExt, StreamExt, TryStreamExt};
use std::ops::Deref;
use std::{
    collections::{hash_map, HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot, watch};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

enum UpdateResult {
    LostAccess,
    LatestUnchanged,
    LatestChanged(TypeId, serde_json::Value),
}

#[derive(Clone)]
pub(crate) struct SavedObject {
    pub have_all_until: Option<Updatedness>,
    pub importance: Importance,
    pub matches_queries: HashSet<QueryId>,
    pub importance_from_queries: Importance,
}

impl SavedObject {
    fn now_have_all_until(&mut self, until: Updatedness) {
        self.have_all_until = std::cmp::max(self.have_all_until, Some(until));
    }
}

type SavedObjectMap = HashMap<ObjectId, SavedObject>;
type SavedQueriesMap = HashMap<QueryId, SavedQuery>;
type QueryUpdatesBroadcastMap =
    HashMap<QueryId, (broadcast::Sender<ObjectId>, broadcast::Receiver<ObjectId>)>;

pub struct ClientDb<LocalDb: ClientSideDb> {
    user: RwLock<Option<User>>,
    ulid: Mutex<ulid::Generator>,
    api: Arc<ApiDb<LocalDb>>,
    db: Arc<CacheDb<LocalDb>>,
    // The `Lock` here is ONLY the accumulated queries lock for this object! The object lock is
    // handled directly in the database only.
    saved_objects: Arc<Mutex<SavedObjectMap>>,
    saved_queries: Arc<Mutex<SavedQueriesMap>>,
    data_saver: mpsc::UnboundedSender<DataSaverMessage>,
    data_saver_skipper: watch::Sender<bool>,
    updates_broadcastee: broadcast::Receiver<ObjectId>,
    query_updates_broadcastees: Arc<Mutex<QueryUpdatesBroadcastMap>>,
    vacuum_guard: Arc<tokio::sync::RwLock<()>>,
    _cleanup_token: tokio_util::sync::DropGuard,
}

const BROADCAST_CHANNEL_SIZE: usize = 64;

enum DataSaverMessage {
    Data {
        update: Arc<Update>,
        now_have_all_until: Option<Updatedness>,
        additional_importance: Importance,
        reply: oneshot::Sender<crate::Result<UpdateResult>>,
    },
    StopFrame(oneshot::Sender<()>),
    ResumeFrame,
}

impl<LocalDb: ClientSideDb> ClientDb<LocalDb> {
    pub async fn connect<C, RRL, EH, EHF, VS>(
        config: C,
        db: LocalDb,
        cache_watermark: usize,
        require_relogin: RRL,
        error_handler: EH,
        vacuum_schedule: ClientVacuumSchedule<VS>,
    ) -> crate::Result<(Arc<ClientDb<LocalDb>>, impl waaaa::Future<Output = usize>)>
    where
        C: crdb_core::Config,
        RRL: 'static + waaaa::Send + Fn(),
        EH: 'static + waaaa::Send + Fn(Upload, crate::Error) -> EHF,
        EHF: 'static + waaaa::Future<Output = OnError>,
        VS: 'static + Send + Fn(ClientStorageInfo) -> bool,
    {
        let _ = config; // mark used
        C::check_ulids();
        let (updates_broadcaster, updates_broadcastee) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let db = Arc::new(CacheDb::new(db, cache_watermark));
        let maybe_login = db
            .get_saved_login()
            .await
            .wrap_context("retrieving saved login info")?;
        if maybe_login.is_none() {
            (require_relogin)();
        }
        let saved_queries = db
            .get_saved_queries()
            .await
            .wrap_context("listing saved queries")?;
        let saved_objects = stream::iter(
            db.get_saved_objects()
                .await
                .wrap_context("listing saved objects")?,
        )
        .map(async |(object_id, saved)| -> crate::Result<_> {
            // TODO(perf-high): replace get_latest with get_json wherever possible
            let value = db.get_json(object_id, Importance::NONE).await?;
            let (matches_queries, importance_from_queries) =
                queries_for(&saved_queries, saved.type_id, &value);
            Ok((
                object_id,
                SavedObject {
                    have_all_until: saved.have_all_until,
                    importance: saved.importance,
                    matches_queries,
                    importance_from_queries,
                },
            ))
        })
        .buffer_unordered(16)
        .try_collect::<HashMap<_, _>>()
        .await?;
        let query_updates_broadcastees = Arc::new(Mutex::new(
            saved_queries
                .keys()
                .map(|&q| (q, broadcast::channel(BROADCAST_CHANNEL_SIZE)))
                .collect(),
        ));
        let saved_queries = Arc::new(Mutex::new(saved_queries));
        let saved_objects = Arc::new(Mutex::new(saved_objects));
        let (api, updates_receiver) = ApiDb::new::<C, _, _, _, _, _>(
            db.clone(),
            {
                let saved_objects = saved_objects.clone();
                move || {
                    // TODO(api-highest): rework this to return all objects and not only subscribed objects
                    // We want all objects to get sent to the server with updatedness information, so that
                    // the server does not re-send it all to us when we already have it. But it'll mean the
                    // user of these callbacks must filter on subscription status to define whether it wants
                    // to subscribe on the object or not.
                    // The same does not need to be done for saved_queries below, that will have no impact on
                    // any further subscription.
                    saved_objects
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(_, o)| o.importance.subscribe())
                        .map(|(id, o)| (*id, o.clone()))
                        .collect()
                }
            },
            {
                let saved_queries = saved_queries.clone();
                move || {
                    saved_queries
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(_, q)| q.importance.subscribe())
                        .map(|(id, q)| (*id, q.clone()))
                        .collect()
                }
            },
            error_handler,
            require_relogin,
        )
        .await?;
        let api = Arc::new(api);
        let cancellation_token = CancellationToken::new();
        let (data_saver, data_saver_receiver) = mpsc::unbounded();
        let this = Arc::new(ClientDb {
            user: RwLock::new(maybe_login.as_ref().map(|l| l.user)),
            ulid: Mutex::new(ulid::Generator::new()),
            api,
            db,
            saved_objects,
            saved_queries,
            data_saver,
            data_saver_skipper: watch::channel(false).0,
            updates_broadcastee,
            query_updates_broadcastees,
            vacuum_guard: Arc::new(tokio::sync::RwLock::new(())),
            _cleanup_token: cancellation_token.clone().drop_guard(),
        });
        this.setup_update_watcher(updates_receiver);
        this.setup_autovacuum(vacuum_schedule, cancellation_token);
        this.setup_data_saver::<C>(data_saver_receiver, updates_broadcaster);
        let (upgrade_finished_sender, upgrade_finished) = oneshot::channel();
        waaaa::spawn({
            let db = this.db.clone();
            async move {
                let num_errors = C::reencode_old_versions(&*db).await;
                let _ = upgrade_finished_sender.send(num_errors);
            }
        });
        if let Some(login_info) = maybe_login {
            this.login(login_info.url, login_info.user, login_info.token)
                .await
                .wrap_context("relogging in as previously logged-in user")?;
        }
        Ok((
            this,
            upgrade_finished.map(|res| res.expect("upgrade task was killed")),
        ))
    }

    pub fn make_ulid(self: &Arc<Self>) -> Ulid {
        // TODO(blocked): replace with generate_overflowing once https://github.com/dylanhart/ulid-rs/pull/75 lands
        self.ulid
            .lock()
            .unwrap()
            .generate()
            .expect("Failed to generate ulid")
    }

    pub fn listen_for_all_updates(self: &Arc<Self>) -> broadcast::Receiver<ObjectId> {
        self.updates_broadcastee.resubscribe()
    }

    pub fn listen_for_updates_on(
        self: &Arc<Self>,
        q: QueryId,
    ) -> Option<broadcast::Receiver<ObjectId>> {
        self.query_updates_broadcastees
            .lock()
            .unwrap()
            .get(&q)
            .map(|(_, recv)| recv.resubscribe())
    }

    fn setup_update_watcher(
        self: &Arc<Self>,
        mut updates_receiver: mpsc::UnboundedReceiver<Updates>,
    ) {
        // No need for a cancellation token: this task will automatically end as soon as the stream
        // coming from `ApiDb` closes, which will happen when `ApiDb` gets dropped.
        waaaa::spawn({
            let data_saver = self.data_saver.clone();
            async move {
                while let Some(updates) = updates_receiver.next().await {
                    for update in updates.data {
                        let (reply, _) = oneshot::channel();
                        data_saver
                            .unbounded_send(DataSaverMessage::Data {
                                update,
                                now_have_all_until: Some(updates.now_have_all_until),
                                additional_importance: Importance::NONE,
                                reply,
                            })
                            .expect("data saver thread cannot go away");
                        // Ignore the result of saving
                    }
                }
            }
        });
    }

    fn setup_autovacuum<F: 'static + Send + Fn(ClientStorageInfo) -> bool>(
        self: &Arc<Self>,
        vacuum_schedule: ClientVacuumSchedule<F>,
        cancellation_token: CancellationToken,
    ) {
        let db = self.db.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        let saved_objects = self.saved_objects.clone();
        let saved_queries = self.saved_queries.clone();
        let api = self.api.clone();
        waaaa::spawn(async move {
            loop {
                match db.storage_info().await {
                    Ok(storage_info) => {
                        if (vacuum_schedule.filter)(storage_info) {
                            let _lock = vacuum_guard.write().await;
                            let to_unsubscribe = Arc::new(Mutex::new(HashSet::new()));
                            if let Err(err) = db
                                .client_vacuum(
                                    {
                                        let to_unsubscribe = to_unsubscribe.clone();
                                        move |object_id| {
                                            to_unsubscribe.lock().unwrap().insert(object_id);
                                        }
                                    },
                                    {
                                        let saved_queries = saved_queries.clone();
                                        let api = api.clone();
                                        move |query_id| {
                                            saved_queries.lock().unwrap().remove(&query_id);
                                            api.unsubscribe_query(query_id);
                                        }
                                    },
                                )
                                .await
                            {
                                tracing::error!(?err, "error occurred while vacuuming");
                            }
                            let mut to_unsubscribe = to_unsubscribe.lock().unwrap();
                            if !to_unsubscribe.is_empty() {
                                {
                                    let mut saved_objects = saved_objects.lock().unwrap();
                                    for object_id in to_unsubscribe.iter() {
                                        saved_objects.remove(object_id);
                                    }
                                }
                                api.unsubscribe(std::mem::take(&mut to_unsubscribe));
                            }
                        }
                    }
                    Err(err) => tracing::error!(
                        ?err,
                        "failed recovering storage info to check for vacuumability"
                    ),
                };

                tokio::select! {
                    _ = waaaa::sleep(vacuum_schedule.frequency) => (),
                    _ = cancellation_token.cancelled() => break,
                }
            }
        });
    }

    fn setup_data_saver<C: crdb_core::Config>(
        self: &Arc<Self>,
        data_receiver: mpsc::UnboundedReceiver<DataSaverMessage>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        let db = self.db.clone();
        let api = self.api.clone();
        let saved_objects = self.saved_objects.clone();
        let saved_queries = self.saved_queries.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        let query_updates_broadcasters = self.query_updates_broadcastees.clone();
        let data_saver_skipper = self.data_saver_skipper.subscribe();
        waaaa::spawn(async move {
            Self::data_saver::<C>(
                data_receiver,
                data_saver_skipper,
                saved_objects,
                saved_queries,
                vacuum_guard,
                db,
                api,
                updates_broadcaster,
                query_updates_broadcasters,
            )
            .await
        });
    }

    #[allow(clippy::too_many_arguments)] // TODO(misc-low): refactor to have a good struct
    async fn data_saver<C: crdb_core::Config>(
        mut data_receiver: mpsc::UnboundedReceiver<DataSaverMessage>,
        mut data_saver_skipper: watch::Receiver<bool>,
        saved_objects: Arc<Mutex<SavedObjectMap>>,
        saved_queries: Arc<Mutex<SavedQueriesMap>>,
        vacuum_guard: Arc<tokio::sync::RwLock<()>>,
        db: Arc<CacheDb<LocalDb>>,
        api: Arc<ApiDb<LocalDb>>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
        query_updates_broadcasters: Arc<Mutex<QueryUpdatesBroadcastMap>>,
    ) {
        // Handle all updates in-order! Without that, the updatedness checks will get completely borken up
        while let Some(msg) = data_receiver.next().await {
            match msg {
                DataSaverMessage::StopFrame(reply) => {
                    // Skip all messages until we received enough ResumeFrame
                    let _ = reply.send(());
                    let mut depth = 1;
                    while let Some(msg) = data_receiver.next().await {
                        match msg {
                            DataSaverMessage::Data { .. } => (), // Skip until we're no longer stopped
                            DataSaverMessage::StopFrame(reply) => {
                                let _ = reply.send(());
                                depth += 1;
                            }
                            DataSaverMessage::ResumeFrame => {
                                depth -= 1;
                            }
                        }
                        if depth == 0 {
                            break;
                        }
                    }
                }
                DataSaverMessage::ResumeFrame => panic!("data saver protocol violation"),
                DataSaverMessage::Data {
                    update,
                    now_have_all_until,
                    additional_importance,
                    reply,
                } => {
                    if *data_saver_skipper.borrow() {
                        continue;
                    }
                    let _guard = vacuum_guard.read().await; // Do not vacuum while we're inserting new data
                    match Self::save_data::<C>(
                        &vacuum_guard,
                        &db,
                        &saved_objects,
                        &saved_queries,
                        &update,
                        now_have_all_until,
                        additional_importance,
                        &updates_broadcaster,
                        &query_updates_broadcasters,
                    )
                    .await
                    {
                        Ok(res) => {
                            let _ = reply.send(Ok(res));
                        }
                        Err(crate::Error::ObjectDoesNotExist(_)) => {
                            // Ignore this error, because if we received from the server an event with an object that does not exist
                            // locally, it probably means that we recently vacuumed it and unsubscribed, but the server had already
                            // sent us the update.
                        }
                        Err(crate::Error::MissingBinaries(binary_ids)) => {
                            let fetch_binaries_res = tokio::select! {
                                bins = Self::fetch_binaries(binary_ids, &db, &api) => bins,
                                _ = data_saver_skipper.wait_for(|do_skip| *do_skip) => continue,
                            };
                            if let Err(err) = fetch_binaries_res {
                                tracing::error!(
                                    ?err,
                                    ?update,
                                    "failed retrieving required binaries for data the server sent us"
                                );
                                continue;
                            }
                            match Self::save_data::<C>(
                                &vacuum_guard,
                                &db,
                                &saved_objects,
                                &saved_queries,
                                &update,
                                now_have_all_until,
                                additional_importance,
                                &updates_broadcaster,
                                &query_updates_broadcasters,
                            )
                            .await
                            {
                                Ok(res) => {
                                    let _ = reply.send(Ok(res));
                                }
                                Err(err) => {
                                    tracing::error!(
                                        ?err,
                                        ?update,
                                        "unexpected error saving data after fetching the binaries"
                                    );
                                    let _ = reply.send(Err(err));
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(?err, ?update, "unexpected error saving data");
                        }
                    }
                }
            }
        }
    }

    async fn fetch_binaries(
        binary_ids: Vec<BinPtr>,
        db: &CacheDb<LocalDb>,
        api: &ApiDb<LocalDb>,
    ) -> crate::Result<()> {
        let mut bins = stream::iter(binary_ids)
            .map(|binary_id| api.get_binary(binary_id).map(move |bin| (binary_id, bin)))
            .buffer_unordered(16); // TODO(perf-low): is 16 a good number?
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

    #[allow(clippy::too_many_arguments)] // TODO(misc-low): refactor to have a good struct
    async fn save_data<C: crdb_core::Config>(
        vacuum_guard: &tokio::sync::RwLock<()>,
        db: &CacheDb<LocalDb>,
        saved_objects: &Mutex<SavedObjectMap>,
        saved_queries: &Mutex<SavedQueriesMap>,
        u: &Update,
        now_have_all_until: Option<Updatedness>,
        additional_importance: Importance,
        updates_broadcaster: &broadcast::Sender<ObjectId>,
        query_updates_broadcasters: &Mutex<QueryUpdatesBroadcastMap>,
    ) -> crate::Result<UpdateResult> {
        let _lock = vacuum_guard.read().await; // avoid vacuum before setting importance
        let object_id = u.object_id;
        let res = match &u.data {
            UpdateData::Creation {
                type_id,
                created_at,
                snapshot_version,
                data,
            } => match C::recreate(
                db,
                *type_id,
                object_id,
                *created_at,
                *snapshot_version,
                data,
                now_have_all_until,
                additional_importance,
            )
            .await?
            {
                Some(res) => UpdateResult::LatestChanged(*type_id, res),
                None => UpdateResult::LatestUnchanged,
            },
            UpdateData::Event {
                type_id,
                event_id,
                data,
            } => match C::submit(
                db,
                *type_id,
                object_id,
                *event_id,
                data,
                now_have_all_until,
                additional_importance,
            )
            .await?
            {
                Some(res) => UpdateResult::LatestChanged(*type_id, res),
                None => UpdateResult::LatestUnchanged,
            },
            UpdateData::LostReadRights => {
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
                let prev = saved_objects.lock().unwrap().remove(&object_id);
                if let (Some(o), Some(now_have_all_until)) = (prev, now_have_all_until) {
                    if let Err(err) = db
                        .update_queries(&o.matches_queries, now_have_all_until)
                        .await
                    {
                        tracing::error!(
                            ?err,
                            ?o.matches_queries,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                    Self::broadcast_query_updates(
                        object_id,
                        saved_queries,
                        query_updates_broadcasters,
                        &o.matches_queries,
                        Some(now_have_all_until),
                    )?;
                }
            }
            UpdateResult::LatestUnchanged => {
                // No change in the object's latest_snapshot
                if let Some(now_have_all_until) = now_have_all_until {
                    let (queries, set_importance) = {
                        let mut saved_objects = saved_objects.lock().unwrap();
                        let Some(o) = saved_objects.get_mut(&object_id) else {
                            return Err(crate::Error::Other(anyhow!("no change in object for which we received an update, but we had not saved it yet?")));
                        };
                        o.now_have_all_until(now_have_all_until);
                        let queries = o.matches_queries.clone();
                        if !o.importance.contains(additional_importance) {
                            o.importance |= additional_importance;
                            (queries, Some((object_id, o.importance)))
                        } else {
                            (queries, None)
                        }
                    };
                    if let Some((object_id, importance)) = set_importance {
                        db.set_object_importance(object_id, importance)
                            .await
                            .wrap_context("updating importance")?;
                    }
                    if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                        tracing::error!(
                            ?err,
                            ?queries,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                    Self::broadcast_query_updates(
                        object_id,
                        saved_queries,
                        query_updates_broadcasters,
                        &queries,
                        Some(now_have_all_until),
                    )?;
                }
            }
            UpdateResult::LatestChanged(type_id, res) => {
                // Something changed in the object's latest_snapshot
                let (queries_after, importance_from_queries_after) =
                    queries_for(&saved_queries.lock().unwrap(), *type_id, res);
                if let Some(now_have_all_until) = now_have_all_until {
                    let (
                        queries_before,
                        importance_from_queries_before,
                        importance_before,
                        importance_after,
                    ) = match saved_objects.lock().unwrap().entry(object_id) {
                        hash_map::Entry::Occupied(mut o) => {
                            let o = o.get_mut();
                            // Update easy metadata
                            o.now_have_all_until(now_have_all_until);
                            let importance_from_queries_before = o.importance_from_queries;
                            let importance_before = o.importance;
                            o.importance_from_queries = importance_from_queries_after;
                            if !o.importance.contains(additional_importance) {
                                o.importance |= additional_importance;
                            }
                            let importance_after = o.importance;

                            // Update queries and broadcast removals (updates & additions will be broadcast later)
                            let queries_before =
                                std::mem::replace(&mut o.matches_queries, queries_after.clone());
                            Self::broadcast_query_updates(
                                object_id,
                                saved_queries,
                                query_updates_broadcasters,
                                &o.matches_queries,
                                Some(now_have_all_until),
                            )?;

                            (
                                queries_before,
                                importance_from_queries_before,
                                importance_before,
                                importance_after,
                            )
                        }
                        hash_map::Entry::Vacant(v) => {
                            v.insert(SavedObject {
                                have_all_until: Some(now_have_all_until),
                                importance: additional_importance,
                                matches_queries: queries_after.clone(),
                                importance_from_queries: importance_from_queries_after,
                            });
                            (
                                HashSet::new(),
                                Importance::NONE,
                                Importance::NONE,
                                additional_importance,
                            )
                        }
                    };
                    if importance_before != importance_after {
                        db.set_object_importance(object_id, importance_after)
                            .await
                            .wrap_context("updating importance")?;
                    }
                    if importance_from_queries_before != importance_from_queries_after {
                        db.set_importance_from_queries(object_id, importance_from_queries_after)
                            .await
                            .wrap_context("updating query locking status")?;
                    }
                    if let Err(err) = db
                        .update_queries(
                            &queries_before.union(&queries_after).copied().collect(),
                            now_have_all_until,
                        )
                        .await
                    {
                        tracing::error!(
                            ?err,
                            ?queries_before,
                            ?queries_after,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                }
                // Broadcast query updates even if the update was local (ie. without now_have_all_until)
                Self::broadcast_query_updates(
                    object_id,
                    saved_queries,
                    query_updates_broadcasters,
                    &queries_after,
                    now_have_all_until,
                )?;
            }
        }
        if let Err(err) = updates_broadcaster.send(object_id) {
            tracing::error!(?err, ?object_id, "failed broadcasting update");
        }
        Ok(res)
    }

    fn broadcast_query_updates(
        object_id: ObjectId,
        saved_queries: &Mutex<SavedQueriesMap>,
        query_updates_broadcasters: &Mutex<QueryUpdatesBroadcastMap>,
        queries: &HashSet<QueryId>,
        now_have_all_until: Option<Updatedness>,
    ) -> crate::Result<()> {
        let mut saved_queries = saved_queries.lock().unwrap();
        let query_updates_broadcasters = query_updates_broadcasters.lock().unwrap();
        for query_id in queries {
            if let Some((sender, _)) = query_updates_broadcasters.get(query_id) {
                if let Err(err) = sender.send(object_id) {
                    tracing::error!(
                        ?err,
                        ?query_id,
                        ?object_id,
                        "failed broadcasting query update"
                    );
                }
            }
            if let Some(query) = saved_queries.get_mut(query_id) {
                if let Some(u) = now_have_all_until {
                    query.now_have_all_until(u);
                }
            }
        }
        Ok(())
    }

    pub fn on_connection_event(
        self: &Arc<Self>,
        cb: impl 'static + waaaa::Send + waaaa::Sync + Fn(ConnectionEvent),
    ) {
        self.api.on_connection_event(cb)
    }

    /// Note: The fact that `token` is actually a token for the `user` passed at creation of this [`ClientDb`]
    /// is not actually checked, and is assumed to be true. Providing the wrong `user` may lead to object creations
    /// or event submissions being spuriously rejected locally, but will not allow them to succeed remotely anyway.
    pub async fn login(
        self: &Arc<Self>,
        url: Arc<String>,
        user: User,
        token: SessionToken,
    ) -> crate::Result<()> {
        if self
            .user
            .read()
            .unwrap()
            .map(|u| u != user)
            .unwrap_or(false)
        {
            // There was already a user logged in, and it is not this user.
            // Start by doing as though the previous user had logged out: kill everything with fire.
            self.logout().await?;
        }
        *self.user.write().unwrap() = Some(user);
        self.api.login(url.clone(), token);
        self.db.save_login(LoginInfo { url, user, token }).await?;
        Ok(())
    }

    pub async fn logout(&self) -> crate::Result<()> {
        // Log out from server
        self.api.logout();

        // Clear local state
        *self.user.write().unwrap() = None;
        self.saved_objects.lock().unwrap().clear();
        self.saved_queries.lock().unwrap().clear();

        // Pause data saving and clear everything locally
        let (reply, reply_receiver) = oneshot::channel();
        self.data_saver_skipper.send_replace(true);
        let _ = self
            .data_saver
            .unbounded_send(DataSaverMessage::StopFrame(reply));
        let _ = reply_receiver.await;
        self.db.remove_everything().await?;
        self.data_saver_skipper.send_replace(false);
        let _ = self
            .data_saver
            .unbounded_send(DataSaverMessage::ResumeFrame);

        Ok(())
    }

    pub fn user(&self) -> Option<User> {
        *self.user.read().unwrap()
    }

    pub fn watch_upload_queue(&self) -> watch::Receiver<Vec<UploadId>> {
        self.api.watch_upload_queue()
    }

    pub async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        self.db.list_uploads().await
    }

    pub async fn get_upload(
        self: &Arc<Self>,
        upload_id: UploadId,
    ) -> crate::Result<Option<Upload>> {
        self.db.get_upload(upload_id).await
    }

    pub fn rename_session(self: &Arc<Self>, name: String) -> oneshot::Receiver<crate::Result<()>> {
        self.api.rename_session(name)
    }

    pub async fn current_session(&self) -> crate::Result<Session> {
        self.api.current_session().await
    }

    pub async fn list_sessions(&self) -> crate::Result<Vec<Session>> {
        self.api.list_sessions().await
    }

    pub fn disconnect_session(
        self: &Arc<Self>,
        session_ref: SessionRef,
    ) -> oneshot::Receiver<crate::Result<()>> {
        self.api.disconnect_session(session_ref)
    }

    /// Pauses the vacuum until the returned mutex guard is dropped
    pub async fn pause_vacuum(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.vacuum_guard.read().await
    }

    pub async fn set_importance<T: Object>(
        self: &Arc<Self>,
        ptr: DbPtr<T>,
        new_importance: Importance,
    ) -> crate::Result<()> {
        let object_id = ptr.to_object_id();
        let old_importance = std::mem::replace(
            &mut self
                .saved_objects
                .lock()
                .unwrap()
                .get_mut(&object_id)
                .ok_or_else(|| crate::Error::ObjectDoesNotExist(object_id))?
                .importance,
            new_importance,
        );
        if old_importance != new_importance {
            self.db
                .set_object_importance(object_id, new_importance)
                .await?;
            if old_importance.subscribe() && !new_importance.subscribe() {
                let mut set = HashSet::with_capacity(1);
                set.insert(object_id);
                self.api.unsubscribe(set);
            } else if !old_importance.subscribe() && new_importance.subscribe() {
                self.get::<T>(new_importance, ptr).await?;
            }
        }
        Ok(())
    }

    pub fn list_saved_queries(self: &Arc<Self>) -> SavedQueriesMap {
        self.saved_queries.lock().unwrap().clone()
    }

    // Note there's no remove_query method. This is fine, as non-locked queries will be
    // vacuumed on the next vacuum run.
    #[allow(clippy::await_holding_lock)] // TODO(api-highest): likely false positive, but do reinvestigate when finished API rewrite
    pub async fn set_query_importance<T: Object>(
        self: &Arc<Self>,
        query_id: QueryId,
        new_importance: Importance,
    ) -> crate::Result<()> {
        let mut saved_queries = self.saved_queries.lock().unwrap();
        let (old_importance, query) = {
            let Some(saved_query) = saved_queries.get_mut(&query_id) else {
                return Err(crate::Error::QueryDoesNotExist(query_id));
            };
            let old_importance = saved_query.importance;
            if saved_query.importance == new_importance {
                return Ok(()); // Nothing to do
            }
            saved_query.importance = new_importance;
            (old_importance, saved_query.query.clone())
        };
        let importance_changed_objects = {
            // TODO(test-high): fuzz that subscribed_queries/objects always stay in sync with in-database data as well as server data
            let mut saved_objects = self.saved_objects.lock().unwrap();
            let mut importance_changed_objects = Vec::new();
            // Increase importance when requested, decrease only if no other query keeps it up
            for (object_id, o) in saved_objects.iter_mut() {
                if o.matches_queries.contains(&query_id) {
                    let new_importance = o
                        .matches_queries
                        .iter()
                        .map(|query_id| match saved_queries.get(query_id) {
                            Some(q) => q.importance,
                            None => {
                                tracing::error!(?query_id, "matched query was not saved");
                                Importance::NONE
                            }
                        })
                        .fold(Importance::NONE, |a, i| a | i);
                    if new_importance != o.importance_from_queries {
                        o.importance_from_queries = new_importance;
                        importance_changed_objects.push(*object_id);
                    }
                }
            }
            importance_changed_objects
        };
        std::mem::drop(saved_queries);
        if old_importance.subscribe() && !new_importance.subscribe() {
            self.api.unsubscribe_query(query_id);
        } else if !old_importance.subscribe() && new_importance.subscribe() {
            self.query_remote::<T>(new_importance, query_id, query)
                .await?
                .count()
                .await; // Drive the stream to completion
        }
        if old_importance != new_importance {
            self.db
                .set_query_importance(query_id, new_importance, importance_changed_objects)
                .await?;
        }
        Ok(())
    }

    pub async fn create<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        object: Arc<T>,
    ) -> crate::Result<(Obj<T, LocalDb>, impl Future<Output = crate::Result<()>>)> {
        let id = self.make_ulid();
        let object_id = ObjectId(id);
        let completion = self
            .create_with(importance, object_id, EventId(id), object.clone())
            .await?;
        let res = Obj::new(DbPtr::from(object_id), object, self.clone());
        Ok((res, completion))
    }

    pub async fn create_with<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        // TODO(api-highest): unify this with data saver setup (and same for submit_with below)
        let user =
            self.user.read().unwrap().ok_or_else(|| {
                crate::Error::Other(anyhow!("called `submit` with no known user"))
            })?;
        if !object
            .can_create(user, object_id, &*self.db)
            .await
            .wrap_context("checking whether object creation seems to be allowed locally")?
        {
            return Err(crate::Error::Forbidden);
        }
        let _lock = self.vacuum_guard.read().await; // avoid vacuum before setting importance
        let val = self
            .db
            .create(
                object_id,
                created_at,
                object.clone(),
                None, // Locally-created object, has no updatedness yet
                importance,
            )
            .await?;
        let do_subscribe = if let Some(val) = val {
            let val_json =
                serde_json::to_value(val).wrap_context("serializing new last snapshot")?;
            let (matches_queries, importance_from_queries) = queries_for(
                &self.saved_queries.lock().unwrap(),
                *T::type_ulid(),
                &val_json,
            );
            self.saved_objects.lock().unwrap().insert(
                object_id,
                SavedObject {
                    have_all_until: None,
                    importance,
                    matches_queries,
                    importance_from_queries,
                },
            );
            if importance_from_queries != Importance::NONE {
                self.db
                    .set_importance_from_queries(object_id, importance_from_queries)
                    .await
                    .wrap_context("updating queries locks")?;
            }
            importance.subscribe() || importance_from_queries.subscribe()
        } else {
            importance.subscribe()
        };
        self.api
            .create(object_id, created_at, object, do_subscribe)
            .await
    }

    pub async fn submit<T: Object>(
        self: &Arc<Self>,
        ptr: DbPtr<T>,
        event: T::Event,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let event_id = EventId(self.make_ulid());
        self.submit_with::<T>(
            Importance::NONE,
            ptr.to_object_id(),
            event_id,
            Arc::new(event),
        )
        .await
    }

    /// Note: this will fail if the object is not subscribed upon yet: it would not make sense anyway.
    pub async fn submit_with<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        let user =
            self.user.read().unwrap().ok_or_else(|| {
                crate::Error::Other(anyhow!("called `submit` with no known user"))
            })?;
        let _lock = self.vacuum_guard.read().await; // avoid vacuum before setting queries lock
        let object = self.db.get_latest::<T>(object_id, Importance::NONE).await?;
        if !object
            .can_apply(user, object_id, &event, &*self.db)
            .await
            .wrap_context("checking whether object creation seems to be allowed locally")?
        {
            return Err(crate::Error::Forbidden);
        }
        let val = self
            .db
            .submit::<T>(
                object_id,
                event_id,
                event.clone(),
                None, // Locally-submitted event, has no updatedness yet
                importance,
            )
            .await?;
        let do_subscribe = if let Some(val) = val {
            let val_json =
                serde_json::to_value(val).wrap_context("serializing new last snapshot")?;
            let (queries, importance_from_queries_after) = queries_for(
                &self.saved_queries.lock().unwrap(),
                *T::type_ulid(),
                &val_json,
            );
            let do_subscribe = importance.subscribe() || importance_from_queries_after.subscribe();
            let importance_from_queries_before = {
                let mut saved_objects = self.saved_objects.lock().unwrap();
                let o = saved_objects.get_mut(&object_id).ok_or_else(|| {
                    crate::Error::Other(anyhow!("Submitted event to non-saved object"))
                })?;
                o.matches_queries = queries;
                let importance_from_queries_before = o.importance_from_queries;
                o.importance_from_queries = importance_from_queries_after;
                importance_from_queries_before
            };
            if importance_from_queries_before != importance_from_queries_after {
                self.db
                    .set_importance_from_queries(object_id, importance_from_queries_after)
                    .await
                    .wrap_context("updating importance from queries")?;
            }
            do_subscribe
        } else {
            importance.subscribe()
        };
        // TODO(api-high): add Subscribe and Lock values (the struct, that also has info about queries) to Obj
        self.api
            .submit::<T>(object_id, event_id, event, do_subscribe)
            .await
    }

    pub async fn get<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        ptr: DbPtr<T>,
    ) -> crate::Result<Obj<T, LocalDb>> {
        let object_id = ptr.to_object_id();
        match self.db.get_latest::<T>(object_id, importance).await {
            Ok(r) => return Ok(Obj::new(ptr, r, self.clone())),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let data = self.api.get(object_id, importance.subscribe()).await?;
        let res = save_object_data_locally::<T, _>(
            data,
            &self.data_saver,
            &self.db,
            importance,
            &self.saved_objects,
            &self.saved_queries,
        )
        .await?;
        Ok(Obj::new(ptr, res, self.clone()))
    }

    pub async fn get_local<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        ptr: DbPtr<T>,
    ) -> crate::Result<Option<Obj<T, LocalDb>>> {
        let object_id = ptr.to_object_id();
        match self.db.get_latest::<T>(object_id, importance).await {
            Ok(res) => Ok(Some(Obj::new(ptr, res, self.clone()))),
            Err(crate::Error::ObjectDoesNotExist(o)) if o == object_id => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn query_local<'a, T: Object>(
        self: &'a Arc<Self>,
        query: Arc<Query>,
    ) -> crate::Result<impl 'a + waaaa::Stream<Item = crate::Result<Obj<T, LocalDb>>>> {
        let object_ids = self
            .db
            .client_query(*T::type_ulid(), query)
            .await
            .wrap_context("listing objects matching query")?;

        Ok(async_stream::stream! {
            for object_id in object_ids {
                match self.db.get_latest::<T>(object_id, Importance::NONE).await {
                    Ok(res) => yield Ok(Obj::new(DbPtr::from(object_id), res, self.clone())),
                    // Ignore missing objects, they were just vacuumed between listing and getting
                    Err(crate::Error::ObjectDoesNotExist(id)) if id == object_id => continue,
                    Err(err) => yield Err(err),
                }
            }
        })
    }

    // TODO(client-med): should we somehow expose only_updated_since / now_have_all_until?
    /// Note that it is assumed here that the same QueryId will always be associated with the same Query.
    /// In particular, this means that when bumping an Object's snapshot_version and adjusting the queries
    /// accordingly, you should change the QueryId, as well as unsubscribe/resubscribe on startup so that
    /// the database gets updated.
    pub async fn query_remote<'a, T: Object>(
        self: &'a Arc<Self>,
        importance: Importance,
        query_id: QueryId,
        query: Arc<Query>,
    ) -> crate::Result<impl 'a + waaaa::Stream<Item = crate::Result<Obj<T, LocalDb>>>> {
        self.query_updates_broadcastees
            .lock()
            .unwrap()
            .entry(query_id)
            .or_insert_with(|| broadcast::channel(BROADCAST_CHANNEL_SIZE));
        let only_updated_since = {
            let mut subscribed_queries = self.saved_queries.lock().unwrap();
            let entry = subscribed_queries.entry(query_id);
            match entry {
                hash_map::Entry::Occupied(mut q) => {
                    if !q.get().importance.lock() && importance.lock() {
                        // Increasing the lock behavior. Re-subscribe from scratch
                        q.get_mut().importance |= importance;
                        q.get_mut().have_all_until = None; // Force have_all_until to 0 as previous stuff could have been vacuumed
                        None
                    } else {
                        // The query was already locked enough. Just proceed.
                        q.get().have_all_until
                    }
                }
                hash_map::Entry::Vacant(v) => {
                    v.insert(SavedQuery {
                        query: query.clone(),
                        type_id: *T::type_ulid(),
                        have_all_until: None,
                        importance,
                    });
                    None
                }
            }
        };
        // TODO(api-high): add "ephemeral query" option, that does not save the query to the database
        self.db
            .record_query(query_id, query.clone(), *T::type_ulid(), importance)
            .await?;
        Ok(self
            .api
            .query::<T>(query_id, only_updated_since, importance.subscribe(), query)
            .then({
                let data_saver = self.data_saver.clone();
                let db = self.db.clone();
                let saved_objects = self.saved_objects.clone();
                let saved_queries = self.saved_queries.clone();
                move |data| {
                    // TODO(api-high): async closures are now stable?
                    let data_saver = data_saver.clone();
                    let db = db.clone();
                    let saved_objects = saved_objects.clone();
                    let saved_queries = saved_queries.clone();
                    async move {
                        let (data, updatedness) = data?;
                        match data {
                            MaybeObject::NotYetSubscribed(data) => {
                                let object_id = data.object_id;
                                let res = save_object_data_locally::<T, _>(
                                    data,
                                    &data_saver,
                                    &db,
                                    Importance::NONE,
                                    &saved_objects,
                                    &saved_queries,
                                )
                                .await?;
                                if let Some(now_have_all_until) = updatedness {
                                    let mut the_query = HashSet::with_capacity(1);
                                    the_query.insert(query_id);
                                    db.update_queries(&the_query, now_have_all_until).await?;
                                    if let Some(q) =
                                        saved_queries.lock().unwrap().get_mut(&query_id)
                                    {
                                        q.now_have_all_until(now_have_all_until);
                                    }
                                }
                                Ok(Obj::new(DbPtr::from(object_id), res, self.clone()))
                            }
                            MaybeObject::AlreadySubscribed(object_id) => {
                                // TODO(api-highest): also add the query to o.matches_query and o.importance_from_queries,
                                // as well as reset self.db.set_importance_from_queries
                                // TODO(test-high): make sure o.matches_query, o.importance_from_queries, db's
                                // importance_from_queries and db's saved_queries are all aligned
                                // TODO(test-high): make sure o.importance and db's importance are aligned
                                self.db
                                    .get_latest::<T>(object_id, Importance::NONE)
                                    .await
                                    .map(|res| Obj::new(DbPtr::from(object_id), res, self.clone()))
                            }
                        }
                    }
                }
            }))
    }

    // TODO(misc-low): should the client be allowed to request a recreation?

    /// Note: when creating a binary, it can be vacuumed away at any time until an object or
    /// event is added that requires it. As such, you probably want to use `pause_vacuum`
    /// to make sure the created binary is not vacuumed away before the object or event
    /// had enough time to get created.
    pub async fn create_binary(self: &Arc<Self>, data: Arc<[u8]>) -> crate::Result<()> {
        let binary_id = crdb_core::hash_binary(&data);
        self.db.create_binary(binary_id, data.clone()).await
        // Do not create the binary over the API. We'll try uploading the object that requires it when
        // that happens, hoping for the binary to already be known by the server. If it is not, then we'll
        // create the binary there then re-send the object. This avoids spurious binary retransmits, and
        // needs to be handled anyway because the server could have vacuumed between the create_binary and
        // the create_object.
    }

    pub async fn get_binary(
        self: &Arc<Self>,
        binary_id: BinPtr,
    ) -> anyhow::Result<Option<Arc<[u8]>>> {
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

impl ClientVacuumSchedule<fn(ClientStorageInfo) -> bool> {
    pub fn new(frequency: Duration) -> Self {
        ClientVacuumSchedule {
            frequency,
            filter: |_| true,
        }
    }

    pub fn never() -> Self {
        ClientVacuumSchedule {
            frequency: Duration::from_secs(86400),
            filter: |_| false,
        }
    }
}

impl<F> ClientVacuumSchedule<F> {
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

/// Returns the list of queries matching, as well as whether at least one of the
/// queries requires locking the value.
fn queries_for(
    saved_queries: &SavedQueriesMap,
    type_id: TypeId,
    value: &serde_json::Value,
) -> (HashSet<QueryId>, Importance) {
    let mut importance = Importance::NONE;
    let queries = saved_queries
        .iter()
        .filter_map(|(id, q)| {
            if type_id == q.type_id && q.query.matches_json(value) {
                importance |= q.importance;
                Some(*id)
            } else {
                None
            }
        })
        .collect();
    (queries, importance)
}

/// Returns the latest snapshot for the object described by `data`
async fn locally_create_all<T: Object, LocalDb: ClientSideDb>(
    data_saver: &mpsc::UnboundedSender<DataSaverMessage>,
    db: &CacheDb<LocalDb>,
    importance: Importance,
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
        let (reply, reply_receiver) = oneshot::channel();
        latest_snapshot_returns.push(reply_receiver);
        data_saver
            .unbounded_send(DataSaverMessage::Data {
                update: Arc::new(Update {
                    object_id: data.object_id,
                    data: UpdateData::Creation {
                        type_id: data.type_id,
                        created_at,
                        snapshot_version,
                        data: snapshot_data,
                    },
                }),
                now_have_all_until: data.events.is_empty().then_some(data.now_have_all_until),
                additional_importance: importance,
                reply,
            })
            .map_err(|_| crate::Error::Other(anyhow!("Data saver task disappeared early")))?;
    } else if importance != Importance::NONE {
        db.get_latest::<T>(data.object_id, importance)
            .await
            .wrap_context("updating importance for object as requested")?;
    }

    // Then, submit all the events
    let events_len = data.events.len();
    for (i, (event_id, event)) in data.events.into_iter().enumerate() {
        // We already handled the importance of the object just above if requested
        let (reply, reply_receiver) = oneshot::channel();
        latest_snapshot_returns.push(reply_receiver);
        data_saver
            .unbounded_send(DataSaverMessage::Data {
                update: Arc::new(Update {
                    object_id: data.object_id,
                    data: UpdateData::Event {
                        type_id: data.type_id,
                        event_id,
                        data: event,
                    },
                }),
                now_have_all_until: (i + 1 == events_len).then_some(data.now_have_all_until),
                additional_importance: Importance::NONE,
                reply,
            })
            .map_err(|_| crate::Error::Other(anyhow!("Data saver task disappeared early")))?;
    }

    // Finally, retrieve the result
    let mut res = None;
    for receiver in latest_snapshot_returns {
        if let Ok(res_data) = receiver.await {
            match res_data {
                Ok(UpdateResult::LatestChanged(_, res_data)) => res = Some(res_data),
                Ok(_) => (),
                Err(crate::Error::ObjectDoesNotExist(o))
                    if o == data.object_id && !importance.lock() =>
                {
                    // Ignore this error, the object was just vacuumed during creation
                }
                Err(err) => return Err(err).wrap_context("creating in local database"),
            }
        }
    }

    Ok(res)
}

async fn save_object_data_locally<T: Object, LocalDb: ClientSideDb>(
    data: ObjectData,
    data_saver: &mpsc::UnboundedSender<DataSaverMessage>,
    db: &CacheDb<LocalDb>,
    importance: Importance,
    saved_objects: &Mutex<SavedObjectMap>,
    saved_queries: &Mutex<SavedQueriesMap>,
) -> crate::Result<Arc<T>> {
    let now_have_all_until = data.now_have_all_until;
    let object_id = data.object_id;
    let type_id = data.type_id;
    if type_id != *T::type_ulid() {
        return Err(crate::Error::WrongType {
            object_id,
            expected_type_id: *T::type_ulid(),
            real_type_id: type_id,
        });
    }
    let res_json = locally_create_all::<T, _>(data_saver, db, importance, data).await?;
    let (res, res_json) = match res_json {
        Some(res_json) => (
            Arc::new(T::deserialize(&res_json).wrap_context("deserializing snapshot")?),
            res_json,
        ),
        None => {
            let res = db.get_latest::<T>(object_id, importance).await?;
            let res_json = serde_json::to_value(&res).wrap_context("serializing snapshot")?;
            (res, res_json)
        }
    };
    let (queries, importance_from_queries) =
        queries_for(&saved_queries.lock().unwrap(), type_id, &res_json);
    db.set_importance_from_queries(object_id, importance_from_queries)
        .await
        .wrap_context("updating queries lock")?;
    let mut saved_objects = saved_objects.lock().unwrap();
    match saved_objects.entry(object_id) {
        hash_map::Entry::Occupied(mut o) => {
            let o = o.get_mut();
            o.now_have_all_until(now_have_all_until);
            o.importance |= importance;
            o.matches_queries = queries;
            o.importance_from_queries = importance_from_queries;
        }
        hash_map::Entry::Vacant(v) => {
            v.insert(SavedObject {
                have_all_until: Some(now_have_all_until),
                importance,
                matches_queries: queries,
                importance_from_queries,
            });
        }
    }
    Ok(res)
}

// TODO(api-highest): is this really required?
impl<LocalDb: ClientSideDb> Deref for ClientDb<LocalDb> {
    type Target = CacheDb<LocalDb>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}
