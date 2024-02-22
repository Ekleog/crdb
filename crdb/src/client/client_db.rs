use super::{api_db::OnError, connection::ConnectionEvent, ApiDb, LocalDb, LoginInfo};
use crate::{
    api::{ApiConfig, UploadId},
    cache::CacheDb,
    crdb_internal::Lock,
    db_trait::Db,
    error::ResultExt,
    future::{CrdbSend, CrdbSync},
    ids::QueryId,
    messages::{MaybeObject, MaybeSnapshot, ObjectData, Update, UpdateData, Updates, Upload},
    object::parse_snapshot_ref,
    BinPtr, CrdbFuture, CrdbStream, DbPtr, EventId, Importance, Obj, Object, ObjectId, Query,
    Session, SessionRef, SessionToken, TypeId, Updatedness, User,
};
use anyhow::anyhow;
use futures::{channel::mpsc, future::Either, stream, FutureExt, StreamExt};
use std::{
    collections::{hash_map, HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::{broadcast, oneshot, watch};
use tokio_util::sync::CancellationToken;

enum UpdateResult {
    LostAccess,
    LatestUnchanged,
    LatestChanged(TypeId, serde_json::Value),
}

pub struct ClientDb {
    user: Arc<RwLock<Option<User>>>,
    api: Arc<ApiDb>,
    db: Arc<CacheDb<LocalDb>>,
    db_bypass: Arc<LocalDb>,
    // The `Lock` here is ONLY the accumulated queries lock for this object! The object lock is
    // handled directly in the database only.
    subscribed_objects:
        Arc<Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>, Lock)>>>,
    subscribed_queries:
        Arc<Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>>>,
    data_saver: mpsc::UnboundedSender<DataSaverMessage>,
    data_saver_skipper: watch::Sender<bool>,
    updates_broadcastee: broadcast::Receiver<ObjectId>,
    query_updates_broadcastees:
        Arc<Mutex<HashMap<QueryId, (broadcast::Sender<ObjectId>, broadcast::Receiver<ObjectId>)>>>,
    vacuum_guard: Arc<tokio::sync::RwLock<()>>,
    _cleanup_token: tokio_util::sync::DropGuard,
}

const BROADCAST_CHANNEL_SIZE: usize = 64;

enum DataSaverMessage {
    Data {
        update: Arc<Update>,
        now_have_all_until: Option<Updatedness>,
        lock: Lock,
        reply: oneshot::Sender<crate::Result<UpdateResult>>,
    },
    StopFrame(oneshot::Sender<()>),
    ResumeFrame,
}

impl ClientDb {
    pub async fn new<C: ApiConfig, RRL, EH, EHF, VS>(
        local_db: &str,
        cache_watermark: usize,
        require_relogin: RRL,
        error_handler: EH,
        vacuum_schedule: ClientVacuumSchedule<VS>,
    ) -> anyhow::Result<(ClientDb, impl CrdbFuture<Output = usize>)>
    where
        C: ApiConfig,
        RRL: 'static + CrdbSend + Fn(),
        EH: 'static + CrdbSend + Fn(Upload, crate::Error) -> EHF,
        EHF: 'static + CrdbFuture<Output = OnError>,
        VS: 'static + Send + Fn(ClientStorageInfo) -> bool,
    {
        C::check_ulids();
        let (updates_broadcaster, updates_broadcastee) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let db_bypass = Arc::new(LocalDb::connect(local_db).await?);
        let db = Arc::new(CacheDb::new(db_bypass.clone(), cache_watermark));
        let maybe_login = db_bypass
            .get_saved_login()
            .await
            .wrap_context("retrieving saved login info")?;
        if maybe_login.is_none() {
            (require_relogin)();
        }
        let subscribed_queries = db_bypass
            .get_subscribed_queries()
            .await
            .wrap_context("listing subscribed queries")?;
        let subscribed_objects = db_bypass
            .get_subscribed_objects()
            .await
            .wrap_context("listing subscribed objects")?
            .into_iter()
            .map(|(object_id, (type_id, value, updatedness))| {
                let (queries, lock) = queries_for(&subscribed_queries, type_id, &value);
                (object_id, (updatedness, queries, lock))
            })
            .collect::<HashMap<_, _>>();
        let query_updates_broadcastees = Arc::new(Mutex::new(
            subscribed_queries
                .keys()
                .map(|&q| (q, broadcast::channel(BROADCAST_CHANNEL_SIZE)))
                .collect(),
        ));
        let subscribed_queries = Arc::new(Mutex::new(subscribed_queries));
        let subscribed_objects = Arc::new(Mutex::new(subscribed_objects));
        let (api, updates_receiver) = ApiDb::new::<C, _, _, _, _, _, _>(
            db_bypass.clone(),
            {
                let subscribed_objects = subscribed_objects.clone();
                move || {
                    subscribed_objects
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(id, (upd, _, _))| (*id, *upd))
                        .collect()
                }
            },
            {
                let subscribed_queries = subscribed_queries.clone();
                move || subscribed_queries.lock().unwrap().clone()
            },
            db.clone(),
            error_handler,
            require_relogin,
        )
        .await?;
        let api = Arc::new(api);
        let cancellation_token = CancellationToken::new();
        let (data_saver, data_saver_receiver) = mpsc::unbounded();
        let this = ClientDb {
            user: Arc::new(RwLock::new(maybe_login.as_ref().map(|l| l.user))),
            api,
            db,
            db_bypass,
            subscribed_objects,
            subscribed_queries,
            data_saver,
            data_saver_skipper: watch::channel(false).0,
            updates_broadcastee,
            query_updates_broadcastees,
            vacuum_guard: Arc::new(tokio::sync::RwLock::new(())),
            _cleanup_token: cancellation_token.clone().drop_guard(),
        };
        this.setup_update_watcher(updates_receiver);
        this.setup_autovacuum(vacuum_schedule, cancellation_token);
        this.setup_data_saver::<C>(data_saver_receiver, updates_broadcaster);
        let (upgrade_finished_sender, upgrade_finished) = oneshot::channel();
        crate::spawn({
            let db = this.db_bypass.clone();
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

    pub fn listen_for_all_updates(&self) -> broadcast::Receiver<ObjectId> {
        self.updates_broadcastee.resubscribe()
    }

    pub fn listen_for_updates_on(&self, q: QueryId) -> Option<broadcast::Receiver<ObjectId>> {
        self.query_updates_broadcastees
            .lock()
            .unwrap()
            .get(&q)
            .map(|(_, recv)| recv.resubscribe())
    }

    fn setup_update_watcher(&self, mut updates_receiver: mpsc::UnboundedReceiver<Updates>) {
        // No need for a cancellation token: this task will automatically end as soon as the stream
        // coming from `ApiDb` closes, which will happen when `ApiDb` gets dropped.
        crate::spawn({
            let data_saver = self.data_saver.clone();
            async move {
                while let Some(updates) = updates_receiver.next().await {
                    for update in updates.data {
                        let (reply, _) = oneshot::channel();
                        data_saver
                            .unbounded_send(DataSaverMessage::Data {
                                update,
                                now_have_all_until: Some(updates.now_have_all_until),
                                lock: Lock::NONE,
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
        &self,
        vacuum_schedule: ClientVacuumSchedule<F>,
        cancellation_token: CancellationToken,
    ) {
        let db_bypass = self.db_bypass.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        let subscribed_objects = self.subscribed_objects.clone();
        let subscribed_queries = self.subscribed_queries.clone();
        let api = self.api.clone();
        crate::spawn(async move {
            loop {
                match db_bypass.storage_info().await {
                    Ok(storage_info) => {
                        if (vacuum_schedule.filter)(storage_info) {
                            let _lock = vacuum_guard.write().await;
                            let to_unsubscribe = Arc::new(Mutex::new(HashSet::new()));
                            if let Err(err) = db_bypass
                                .vacuum(
                                    {
                                        let to_unsubscribe = to_unsubscribe.clone();
                                        move |object_id| {
                                            to_unsubscribe.lock().unwrap().insert(object_id);
                                        }
                                    },
                                    {
                                        let subscribed_queries = subscribed_queries.clone();
                                        let api = api.clone();
                                        move |query_id| {
                                            subscribed_queries.lock().unwrap().remove(&query_id);
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
                                    let mut subscribed_objects = subscribed_objects.lock().unwrap();
                                    for object_id in to_unsubscribe.iter() {
                                        subscribed_objects.remove(object_id);
                                    }
                                }
                                api.unsubscribe(std::mem::replace(
                                    &mut to_unsubscribe,
                                    HashSet::new(),
                                ));
                            }
                        }
                    }
                    Err(err) => tracing::error!(
                        ?err,
                        "failed recovering storage info to check for vacuumability"
                    ),
                };

                tokio::select! {
                    _ = crate::sleep(vacuum_schedule.frequency) => (),
                    _ = cancellation_token.cancelled() => break,
                }
            }
        });
    }

    fn setup_data_saver<C: ApiConfig>(
        &self,
        data_receiver: mpsc::UnboundedReceiver<DataSaverMessage>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
    ) {
        let db_bypass = self.db_bypass.clone();
        let api = self.api.clone();
        let subscribed_objects = self.subscribed_objects.clone();
        let subscribed_queries = self.subscribed_queries.clone();
        let vacuum_guard = self.vacuum_guard.clone();
        let query_updates_broadcasters = self.query_updates_broadcastees.clone();
        let data_saver_skipper = self.data_saver_skipper.subscribe();
        crate::spawn(async move {
            Self::data_saver::<C>(
                data_receiver,
                data_saver_skipper,
                subscribed_objects,
                subscribed_queries,
                vacuum_guard,
                db_bypass,
                api,
                updates_broadcaster,
                query_updates_broadcasters,
            )
            .await
        });
    }

    async fn data_saver<C: ApiConfig>(
        mut data_receiver: mpsc::UnboundedReceiver<DataSaverMessage>,
        mut data_saver_skipper: watch::Receiver<bool>,
        subscribed_objects: Arc<
            Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>, Lock)>>,
        >,
        subscribed_queries: Arc<
            Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>>,
        >,
        vacuum_guard: Arc<tokio::sync::RwLock<()>>,
        db: Arc<LocalDb>,
        api: Arc<ApiDb>,
        updates_broadcaster: broadcast::Sender<ObjectId>,
        query_updates_broadcasters: Arc<
            Mutex<HashMap<QueryId, (broadcast::Sender<ObjectId>, broadcast::Receiver<ObjectId>)>>,
        >,
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
                    lock,
                    reply,
                } => {
                    if *data_saver_skipper.borrow() {
                        continue;
                    }
                    let _guard = vacuum_guard.read().await; // Do not vacuum while we're inserting new data
                    match Self::save_data::<C>(
                        &db,
                        &subscribed_objects,
                        &subscribed_queries,
                        &update,
                        now_have_all_until,
                        lock,
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
                            // locally, it probably means that we recently unsubscribed from it but the server had already sent us the
                            // update.
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
                                &db,
                                &subscribed_objects,
                                &subscribed_queries,
                                &update,
                                now_have_all_until,
                                lock,
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
        db: &LocalDb,
        api: &ApiDb,
    ) -> crate::Result<()> {
        let mut bins = stream::iter(binary_ids.into_iter())
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

    async fn save_data<C: ApiConfig>(
        db: &LocalDb,
        subscribed_objects: &Mutex<
            HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>, Lock)>,
        >,
        subscribed_queries: &Mutex<
            HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
        >,
        u: &Update,
        now_have_all_until: Option<Updatedness>,
        lock: Lock,
        updates_broadcaster: &broadcast::Sender<ObjectId>,
        query_updates_broadcasters: &Mutex<
            HashMap<QueryId, (broadcast::Sender<ObjectId>, broadcast::Receiver<ObjectId>)>,
        >,
    ) -> crate::Result<UpdateResult> {
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
                &data,
                now_have_all_until,
                lock,
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
                &*db,
                *type_id,
                object_id,
                *event_id,
                &data,
                now_have_all_until,
                lock,
            )
            .await?
            {
                Some(res) => UpdateResult::LatestChanged(*type_id, res),
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
                if let Some((_, queries, _)) = prev {
                    if let Some(now_have_all_until) = now_have_all_until {
                        if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                            tracing::error!(
                                ?err,
                                ?queries,
                                "failed updating now_have_all_until for queries"
                            );
                        }
                        let mut subscribed_queries = subscribed_queries.lock().unwrap();
                        for query_id in queries {
                            if let Some((sender, _)) =
                                query_updates_broadcasters.lock().unwrap().get(&query_id)
                            {
                                if let Err(err) = sender.send(object_id) {
                                    tracing::error!(
                                        ?err,
                                        ?query_id,
                                        ?object_id,
                                        "failed broadcasting query update"
                                    );
                                }
                            }
                            if let Some(query) = subscribed_queries.get_mut(&query_id) {
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
                    for query_id in queries {
                        if let Some((sender, _)) =
                            query_updates_broadcasters.lock().unwrap().get(&query_id)
                        {
                            if let Err(err) = sender.send(object_id) {
                                tracing::error!(
                                    ?err,
                                    ?query_id,
                                    ?object_id,
                                    "failed broadcasting query update"
                                );
                            }
                        }
                        if let Some(query) = subscribed_queries.get_mut(&query_id) {
                            query.2 = std::cmp::max(query.2, Some(now_have_all_until));
                        }
                    }
                }
            }
            UpdateResult::LatestChanged(type_id, res) => {
                // Something changed in the object's latest_snapshot
                if let Some(now_have_all_until) = now_have_all_until {
                    let (mut queries, lock_after) =
                        queries_for(&subscribed_queries.lock().unwrap(), *type_id, res);
                    let lock_before = if let Some((_, queries_before, lock_before)) =
                        subscribed_objects.lock().unwrap().insert(
                            object_id,
                            (Some(now_have_all_until), queries.clone(), lock_after),
                        ) {
                        queries.extend(queries_before);
                        lock_before
                    } else {
                        Lock::NONE
                    };
                    if lock_before != lock_after {
                        db.change_locks(lock_before, lock_after, object_id)
                            .await
                            .wrap_context("updating query locking status")?;
                    }
                    if let Err(err) = db.update_queries(&queries, now_have_all_until).await {
                        tracing::error!(
                            ?err,
                            ?queries,
                            "failed updating now_have_all_until for queries"
                        );
                    }
                    let mut subscribed_queries = subscribed_queries.lock().unwrap();
                    for query_id in queries {
                        if let Some((sender, _)) =
                            query_updates_broadcasters.lock().unwrap().get(&query_id)
                        {
                            if let Err(err) = sender.send(object_id) {
                                tracing::error!(
                                    ?err,
                                    ?query_id,
                                    ?object_id,
                                    "failed broadcasting query update"
                                );
                            }
                        }
                        if let Some(query) = subscribed_queries.get_mut(&query_id) {
                            query.2 = Some(now_have_all_until);
                        }
                    }
                }
            }
        }
        if let Err(err) = updates_broadcaster.send(object_id) {
            tracing::error!(?err, ?object_id, "failed broadcasting update");
        }
        Ok(res)
    }

    pub fn on_connection_event(
        &self,
        cb: impl 'static + CrdbSend + CrdbSync + Fn(ConnectionEvent),
    ) {
        self.api.on_connection_event(cb)
    }

    /// Note: The fact that `token` is actually a token for the `user` passed at creation of this [`ClientDb`]
    /// is not actually checked, and is assumed to be true. Providing the wrong `user` may lead to object creations
    /// or event submissions being spuriously rejected locally, but will not allow them to succeed remotely anyway.
    pub async fn login(
        &self,
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
        self.subscribed_objects.lock().unwrap().clear();
        self.subscribed_queries.lock().unwrap().clear();

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

    pub async fn get_upload(&self, upload_id: UploadId) -> crate::Result<Option<Upload>> {
        self.db.get_upload(upload_id).await
    }

    pub fn rename_session(&self, name: String) -> oneshot::Receiver<crate::Result<()>> {
        self.api.rename_session(name)
    }

    pub async fn current_session(&self) -> crate::Result<Session> {
        self.api.current_session().await
    }

    pub async fn list_sessions(&self) -> crate::Result<Vec<Session>> {
        self.api.list_sessions().await
    }

    pub fn disconnect_session(
        &self,
        session_ref: SessionRef,
    ) -> oneshot::Receiver<crate::Result<()>> {
        self.api.disconnect_session(session_ref)
    }

    pub async fn pause_vacuum(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.vacuum_guard.read().await
    }

    pub async fn unlock(&self, ptr: ObjectId) -> crate::Result<()> {
        self.db.change_locks(Lock::OBJECT, Lock::NONE, ptr).await
    }

    pub async fn unsubscribe(&self, object_ids: HashSet<ObjectId>) -> crate::Result<()> {
        for object_id in object_ids.iter() {
            self.db.remove(*object_id).await?;
            self.subscribed_objects.lock().unwrap().remove(object_id);
        }
        self.api.unsubscribe(object_ids);
        Ok(())
    }

    pub async fn unsubscribe_query(&self, query_id: QueryId) -> crate::Result<()> {
        self.query_updates_broadcastees
            .lock()
            .unwrap()
            .remove(&query_id);
        let objects_to_unlock = {
            // TODO(test-high): fuzz that subscribed_queries/objects always stay in sync with in-database data
            let mut subscribed_queries = self.subscribed_queries.lock().unwrap();
            let Some((_, _, _, removed_query_lock)) = subscribed_queries.remove(&query_id) else {
                return Ok(()); // was not subscribed to the query
            };
            let mut subscribed_objects = self.subscribed_objects.lock().unwrap();
            let mut objects_to_unlock = Vec::new();
            for (object_id, (_, queries, query_lock)) in subscribed_objects.iter_mut() {
                let did_remove = queries.remove(&query_id);
                if did_remove && removed_query_lock != Lock::NONE {
                    let old_query_lock = *query_lock;
                    *query_lock = Lock::NONE;
                    for q in queries.iter() {
                        *query_lock |= subscribed_queries
                            .get(q)
                            .expect("object is subscribed to a non-subscribed queries")
                            .3;
                    }
                    if *query_lock != old_query_lock {
                        debug_assert!(*query_lock | Lock::FOR_QUERIES == old_query_lock);
                        objects_to_unlock.push(*object_id);
                    }
                }
            }
            objects_to_unlock
        };
        self.api.unsubscribe_query(query_id);
        self.db_bypass
            .unsubscribe_query(query_id, objects_to_unlock)
            .await
    }

    pub async fn create<T: Object>(
        &self,
        importance: Importance,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
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
        let _lock = self.vacuum_guard.read().await; // avoid vacuum before setting queries lock
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
        let do_subscribe = if let Some(val) = val {
            let val_json =
                serde_json::to_value(val).wrap_context("serializing new last snapshot")?;
            let (queries, lock) = queries_for(
                &self.subscribed_queries.lock().unwrap(),
                *T::type_ulid(),
                &val_json,
            );
            let do_subscribe = importance >= Importance::Subscribe || !queries.is_empty();
            if do_subscribe {
                self.subscribed_objects
                    .lock()
                    .unwrap()
                    .insert(object_id, (None, queries, lock));
            }
            if lock != Lock::NONE {
                self.db
                    .change_locks(Lock::NONE, lock, object_id)
                    .await
                    .wrap_context("updating queries locks")?;
            }
            do_subscribe
        } else {
            importance >= Importance::Subscribe
        };
        if !do_subscribe {
            self.db.remove(object_id).await?;
        }
        self.api
            .create(object_id, created_at, object, do_subscribe)
            .await
    }

    /// Note: this will fail if the object is not subscribed upon yet: it would not make sense anyway.
    pub async fn submit<T: Object>(
        &self,
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
        let object = self.db.get_latest::<T>(Lock::NONE, object_id).await?;
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
                importance.to_object_lock(),
            )
            .await?;
        let do_subscribe = if let Some(val) = val {
            let val_json =
                serde_json::to_value(val).wrap_context("serializing new last snapshot")?;
            let (queries, lock_after) = queries_for(
                &self.subscribed_queries.lock().unwrap(),
                *T::type_ulid(),
                &val_json,
            );
            let do_subscribe = importance >= Importance::Subscribe || !queries.is_empty();
            let (_, _, lock_before) = self
                .subscribed_objects
                .lock()
                .unwrap()
                .insert(object_id, (None, queries, lock_after))
                .ok_or_else(|| {
                    crate::Error::Other(anyhow!("Submitted event to non-subscribed object"))
                })?;
            if lock_before != lock_after {
                self.db
                    .change_locks(lock_before, lock_after, object_id)
                    .await
                    .wrap_context("updating queries locks")?;
            }
            do_subscribe
        } else {
            importance >= Importance::Subscribe
        };
        // TODO(api-high): consider introducing a ManuallyUpdated importance level, though it will be quite a big refactor
        // TODO(api-high): make Importance actually be two-dimensional, with both subscription level and lock level
        // TODO(api-high): remove Importance::Latest, and just have separate functions for that
        self.api
            .submit::<T>(object_id, event_id, event, do_subscribe)
            .await
    }

    pub async fn get<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        object_id: ObjectId,
    ) -> crate::Result<Obj<T>> {
        let lock = importance.to_object_lock();
        let subscribe = importance.to_subscribe();
        match self.db.get_latest::<T>(lock, object_id).await {
            Ok(r) => return Ok(Obj::new(DbPtr::from(object_id), r, self.clone())),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let res = if subscribe {
            let data = self.api.get_subscribe(object_id).await?;
            let res = save_object_data_locally::<T>(
                data,
                &self.data_saver,
                &self.db,
                lock,
                &self.subscribed_objects,
                &self.subscribed_queries,
            )
            .await?;
            res
        } else {
            let res = self.api.get_latest(object_id).await?;
            let res = parse_snapshot_ref::<T>(res.snapshot_version, &res.snapshot)
                .wrap_context("deserializing server-returned snapshot")?;
            Arc::new(res)
        };
        Ok(Obj::new(DbPtr::from(object_id), res, self.clone()))
    }

    pub async fn get_local<T: Object>(
        self: &Arc<Self>,
        importance: Importance,
        object_id: ObjectId,
    ) -> crate::Result<Option<Obj<T>>> {
        match self
            .db
            .get_latest::<T>(importance.to_object_lock(), object_id)
            .await
        {
            Ok(res) => Ok(Some(Obj::new(DbPtr::from(object_id), res, self.clone()))),
            Err(crate::Error::ObjectDoesNotExist(o)) if o == object_id => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn query_local<'a, T: Object>(
        self: &'a Arc<Self>,
        query: Arc<Query>,
    ) -> crate::Result<impl 'a + CrdbStream<Item = crate::Result<Obj<T>>>> {
        let object_ids = self
            .db
            .query::<T>(query)
            .await
            .wrap_context("listing objects matching query")?;

        Ok(async_stream::stream! {
            for object_id in object_ids {
                match self.db.get_latest::<T>(Lock::NONE, object_id).await {
                    Ok(res) => yield Ok(Obj::new(DbPtr::from(object_id), res, self.clone())),
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
    ) -> crate::Result<impl '_ + CrdbStream<Item = crate::Result<(ObjectId, Arc<T>)>>> {
        if importance >= Importance::Subscribe {
            self.query_updates_broadcastees
                .lock()
                .unwrap()
                .entry(query_id)
                .or_insert_with(|| broadcast::channel(BROADCAST_CHANNEL_SIZE));
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
                    .subscribe_query(
                        query_id,
                        query.clone(),
                        *T::type_ulid(),
                        importance >= Importance::Lock,
                    )
                    .await?;
            }
            Ok(Either::Left(
                self.api
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
                                        let object_id = data.object_id;
                                        let res = save_object_data_locally::<T>(
                                            data,
                                            &data_saver,
                                            &db,
                                            lock,
                                            &subscribed_objects,
                                            &subscribed_queries,
                                        )
                                        .await?;
                                        if let Some(now_have_all_until) = updatedness {
                                            let mut the_query = HashSet::with_capacity(1);
                                            the_query.insert(query_id);
                                            db.update_queries(&the_query, now_have_all_until)
                                                .await?;
                                        }
                                        if let Some(q) =
                                            subscribed_queries.lock().unwrap().get_mut(&query_id)
                                        {
                                            q.2 = updatedness.or(q.2);
                                        }
                                        Ok((object_id, res))
                                    }
                                    MaybeObject::AlreadySubscribed(object_id) => self
                                        .db
                                        .get_latest::<T>(lock, object_id)
                                        .await
                                        .map(|res| (object_id, res)),
                                }
                            }
                        }
                    }),
            ))
        } else {
            // TODO(client-med): should we somehow expose only_updated_since / now_have_all_until?
            Ok(Either::Right(self.api.query_latest::<T>(None, query).then(
                {
                    move |data| async move {
                        let (data, _now_have_all_until) = data?;
                        match data {
                            MaybeSnapshot::NotSubscribed(data) => {
                                let object_id = data.object_id;
                                let res =
                                    parse_snapshot_ref::<T>(data.snapshot_version, &data.snapshot)
                                        .wrap_context("deserializing server-returned snapshot")?;
                                Ok((object_id, Arc::new(res)))
                            }
                            MaybeSnapshot::AlreadySubscribed(object_id) => self
                                .db
                                .get_latest::<T>(Lock::NONE, object_id)
                                .await
                                .map(|res| (object_id, res)),
                        }
                    }
                },
            )))
        }
    }

    // TODO(misc-low): should the client be allowed to request a recreation?

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

fn queries_for(
    subscribed_queries: &HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>,
    type_id: TypeId,
    value: &serde_json::Value,
) -> (HashSet<QueryId>, Lock) {
    let mut lock = Lock::NONE;
    let queries = subscribed_queries
        .iter()
        .filter(|(_, (query, q_type_id, _, query_lock))| {
            if type_id == *q_type_id && query.matches_json(&value) {
                lock |= *query_lock;
                true
            } else {
                false
            }
        })
        .map(|(id, _)| *id)
        .collect();
    (queries, lock)
}

/// Returns the latest snapshot for the object described by `data`
async fn locally_create_all<T: Object>(
    data_saver: &mpsc::UnboundedSender<DataSaverMessage>,
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
                now_have_all_until: data.events.is_empty().then(|| data.now_have_all_until),
                lock,
                reply,
            })
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
                now_have_all_until: (i + 1 == events_len).then(|| data.now_have_all_until),
                lock: Lock::NONE,
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

async fn save_object_data_locally<T: Object>(
    data: ObjectData,
    data_saver: &mpsc::UnboundedSender<DataSaverMessage>,
    db: &CacheDb<LocalDb>,
    lock: Lock,
    subscribed_objects: &Mutex<HashMap<ObjectId, (Option<Updatedness>, HashSet<QueryId>, Lock)>>,
    subscribed_queries: &Mutex<HashMap<QueryId, (Arc<Query>, TypeId, Option<Updatedness>, Lock)>>,
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
    let res_json = locally_create_all::<T>(&data_saver, &db, lock, data).await?;
    let (res, res_json) = match res_json {
        Some(res_json) => (
            Arc::new(T::deserialize(&res_json).wrap_context("deserializing snapshot")?),
            res_json,
        ),
        None => {
            let res = db.get_latest::<T>(Lock::NONE, object_id).await?;
            let res_json = serde_json::to_value(&res).wrap_context("serializing snapshot")?;
            (res, res_json)
        }
    };
    let (queries, lock_after) =
        queries_for(&subscribed_queries.lock().unwrap(), type_id, &res_json);
    if lock_after != lock {
        db.change_locks(Lock::NONE, lock_after, object_id)
            .await
            .wrap_context("updating queries lock")?;
    }
    subscribed_objects
        .lock()
        .unwrap()
        .insert(object_id, (Some(now_have_all_until), queries, lock_after));
    Ok(res)
}
