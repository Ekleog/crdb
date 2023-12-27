use crate::{
    api::{BinPtr, Query},
    db_trait::{Db, EventId, NewEvent, NewObject, NewSnapshot, ObjectId, Timestamp},
    full_object::{DynSized, FullObject},
    hash_binary, Object, User,
};
use anyhow::{anyhow, Context};
use futures::{pin_mut, Stream, StreamExt};
use std::{
    collections::{hash_map, HashMap},
    future::Future,
    sync::Arc,
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ObjectCache {
    objects: HashMap<ObjectId, FullObject>,
    size: usize,
    // TODO: have fuzzers that assert that `size` stays in-sync with `objects`
}

impl ObjectCache {
    fn new() -> ObjectCache {
        ObjectCache {
            objects: HashMap::new(),
            size: 0,
        }
    }

    async fn create_impl<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<(bool, &mut FullObject)> {
        let cache_entry = self.objects.entry(id);
        match cache_entry {
            hash_map::Entry::Occupied(entry) => {
                let o = entry.get();
                let o = o.creation_info();
                anyhow::ensure!(
                    o.created_at == created_at
                        && o.id == id
                        && o.creation
                            .ref_to_any()
                            .downcast_ref::<T>()
                            .map(|v| v == &*object)
                            .unwrap_or(false),
                    "Object {id:?} was already created with a different initial value"
                );
                std::mem::drop(o);
                Ok((false, entry.into_mut()))
            }
            hash_map::Entry::Vacant(v) => {
                let o = FullObject::new(id, created_at, object);
                self.size += o.deep_size_of();
                let res = v.insert(o);
                Ok((true, res))
            }
        }
    }

    /// Returns `true` if the object was newly inserted in the cache, and `false` if
    /// the object was already present in the cache. Errors if the object id was already
    /// in the cache with a different value.
    pub async fn create<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<bool> {
        self.create_impl(id, created_at, object).await.map(|r| r.0)
    }

    async fn remove(&mut self, object_id: &ObjectId) {
        if let Some(o) = self.objects.remove(object_id) {
            self.size -= o.deep_size_of();
        }
    }

    /// Returns `true` if the event was newly inserted in the cache, and `false` if
    /// the event was already present in the cache. Returns an error if another event
    /// with the same id had already been applied, if the event is earlier than the
    /// object's last recreation time, if the provided `T` is wrong or if the database
    /// failed to return the pre-event object.
    ///
    /// If `db` is `Some`, then this will automatically fetch the contents for `object_id`
    /// if it is not in the cache yet.
    pub async fn submit<D: Db, T: Object>(
        &mut self,
        db: Option<&D>,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<bool> {
        match self.objects.entry(object_id) {
            hash_map::Entry::Occupied(object) => {
                let object = object.get();
                self.size -= object.deep_size_of();
                let res = object
                    .apply::<T>(event_id, event)
                    .with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
                self.size += object.deep_size_of();
                Ok(res)
            }
            hash_map::Entry::Vacant(v) => {
                if let Some(db) = db {
                    let o = db
                        .get::<T>(object_id)
                        .await
                        .with_context(|| format!("getting {object_id:?} from database"))?
                        .ok_or_else(|| anyhow!("Submitted an event to object {object_id:?} that does not exist in the db"))?;
                    let res = o
                        .apply::<T>(event_id, event)
                        .with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
                    self.size += o.deep_size_of();
                    v.insert(o);
                    Ok(res)
                } else {
                    Ok(false)
                }
            }
        }
    }

    pub async fn snapshot<T: Object>(
        &mut self,
        object: ObjectId,
        time: Timestamp,
    ) -> anyhow::Result<()> {
        if let Some(o) = self.objects.get_mut(&object) {
            self.size -= o.deep_size_of();
            o.recreate_at::<T>(time)?;
            self.size += o.deep_size_of();
        }
        Ok(())
    }

    fn get(&self, id: &ObjectId) -> Option<&FullObject> {
        self.objects.get(id)
    }

    async fn insert<T: Object>(&mut self, o: FullObject) -> anyhow::Result<()> {
        let (creation, changes) = o.extract_all_clone();
        // Do not directly insert into the hashmap, because the hashmap could already contain more
        // recent events for this object. Instead, pass the object and all the events one by one,
        // to merge with anything that would already exist.
        let (_, created) = self
            .create_impl(
                creation.id,
                creation.created_at,
                creation
                    .creation
                    .arc_to_any()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("Failed to downcast an object to {:?}", T::ulid()))?,
            )
            .await
            .with_context(|| format!("creating object {:?}", creation.id))?;
        let initial_size = created.deep_size_of();
        for (event_id, c) in changes.iter() {
            created
                .apply::<T>(
                    *event_id,
                    c.event
                        .clone()
                        .arc_to_any()
                        .downcast::<T::Event>()
                        .map_err(|_| {
                            anyhow!(
                                "Failed to downcast an event to {:?}'s event type",
                                T::ulid()
                            )
                        })?,
                )
                .with_context(|| format!("applying {event_id:?} on {:?}", creation.id))?;
        }
        let finished_size = created.deep_size_of();
        self.size -= initial_size; // cancel what happened in the `create_impl`
        self.size += finished_size;
        Ok(())
    }
}

pub trait CacheConfig {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `create` method with the proper type and the fields from `o`.
    fn create(
        cache: &mut ObjectCache,
        o: NewObject,
    ) -> impl Send + Future<Output = anyhow::Result<bool>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `submit` method with the proper type and the fields from `o`.
    fn submit<D: Db>(
        db: Option<&D>,
        cache: &mut ObjectCache,
        e: NewEvent,
    ) -> impl Send + Future<Output = anyhow::Result<bool>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `snapshot` method with the proper type and the fields from `s`.
    fn snapshot(
        cache: &mut ObjectCache,
        s: NewSnapshot,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `create` method with the proper type and the fields from `o`.
    fn create_in_db<D: Db>(db: &D, o: NewObject)
        -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `submit` method with the proper type and the fields from `o`.
    fn submit_in_db<D: Db>(db: &D, e: NewEvent) -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `snapshot` method with the proper type and the fields from `s`.
    fn snapshot_in_db<D: Db>(
        db: &D,
        s: NewSnapshot,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
}

struct Binaries {
    data: HashMap<BinPtr, Arc<Vec<u8>>>,
    size: usize,
    // TODO: have fuzzers that assert that `size` stays in-sync with `binaries`
}

impl Binaries {
    fn clear(&mut self) {
        self.data.retain(|_, v| {
            if Arc::strong_count(v) == 1 {
                self.size -= v.len();
                false
            } else {
                true
            }
        })
    }

    fn insert(&mut self, id: BinPtr, value: Arc<Vec<u8>>) {
        self.size += value.len();
        self.data.insert(id, value);
    }

    fn get(&self, id: &BinPtr) -> Option<Arc<Vec<u8>>> {
        self.data.get(id).cloned()
    }
}

pub(crate) struct Cache<D: Db> {
    db: Arc<D>,
    // TODO: figure out how to purge from cache (LRU-style), using DeepSizeOf
    cache: Arc<RwLock<ObjectCache>>,
    binaries: Arc<RwLock<Binaries>>,
}

impl<D: Db> Cache<D> {
    fn watch_from<C: CacheConfig, OtherDb: Db>(&self, db: &Arc<OtherDb>, relay_to_db: bool) {
        // Watch new objects
        tokio::task::spawn({
            let db = db.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            async move {
                let new_objects = db.new_objects().await;
                pin_mut!(new_objects);
                while let Some(o) = new_objects.next().await {
                    let mut cache = cache.write().await;
                    let object = o.id;
                    if relay_to_db {
                        if let Err(error) = C::create_in_db(&*internal_db, o.clone()).await {
                            tracing::error!(
                                ?error,
                                ?object,
                                "failed creating received object in internal db"
                            );
                        }
                    }
                    if let Err(error) = C::create(&mut *cache, o).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed creating received object in cache"
                        );
                    }
                }
            }
        });

        // Watch new events
        tokio::task::spawn({
            let db = db.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            async move {
                let new_events = db.new_events().await;
                pin_mut!(new_events);
                while let Some(e) = new_events.next().await {
                    let mut cache = cache.write().await;
                    let object = e.object_id;
                    let event = e.id;
                    if relay_to_db {
                        if let Err(error) = C::submit_in_db(&*internal_db, e.clone()).await {
                            tracing::error!(
                                ?error,
                                ?object,
                                ?event,
                                "failed submitting received object to internal db"
                            );
                        }
                    }
                    // DO NOT re-fetch object when receiving an event not in cache for it.
                    // Without this, users would risk unsubscribing from an object, then receiving
                    // an event on this object (as a race condition), and then staying subscribed.
                    if let Err(error) = C::submit::<D>(None, &mut *cache, e).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            ?event,
                            "failed submitting received event to cache"
                        );
                    }
                }
            }
        });

        // Watch new snapshots
        tokio::task::spawn({
            let db = db.clone();
            let internal_db = self.db.clone();
            let cache = self.cache.clone();
            async move {
                let new_snapshots = db.new_snapshots().await;
                pin_mut!(new_snapshots);
                while let Some(s) = new_snapshots.next().await {
                    let mut cache = cache.write().await;
                    let object = s.object_id;
                    if relay_to_db {
                        if let Err(error) = C::snapshot_in_db(&*internal_db, s.clone()).await {
                            tracing::error!(
                                ?error,
                                ?object,
                                "failed snapshotting as per received event in internal db"
                            );
                        }
                    }
                    if let Err(error) = C::snapshot(&mut *cache, s).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed snapshotting as per received event in cache"
                        )
                    }
                }
            }
        });
    }

    pub(crate) fn new<C: CacheConfig>(db: Arc<D>) -> Cache<D> {
        let cache = Arc::new(RwLock::new(ObjectCache::new()));
        let this = Cache {
            db: db.clone(),
            cache,
            binaries: Arc::new(RwLock::new(Binaries {
                data: HashMap::new(),
                size: 0,
            })),
        };
        this.watch_from::<C, _>(&db, false);
        this
    }

    /// Relays all new objects/events from `db` to the internal database, caching them in the process.
    pub(crate) fn also_watch_from<C: CacheConfig, OtherDb: Db>(&self, db: &Arc<OtherDb>) {
        self.watch_from::<C, _>(db, true)
    }

    pub(crate) async fn clear_cache(&self) {
        self.clear_binaries_cache().await;
        // TODO: clear cache
    }

    pub(crate) async fn clear_binaries_cache(&self) {
        self.binaries.write().await.clear();
    }
}

impl<D: Db> Db for Cache<D> {
    async fn new_objects(&self) -> impl Stream<Item = NewObject> {
        self.db.new_objects().await
    }

    async fn new_events(&self) -> impl Stream<Item = NewEvent> {
        self.db.new_events().await
    }

    async fn new_snapshots(&self) -> impl Stream<Item = NewSnapshot> {
        self.db.new_snapshots().await
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        cache.remove(&ptr).await;
        self.db.unsubscribe(ptr).await
    }

    async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        if cache.create(id, created_at, object.clone()).await? {
            self.db.create(id, created_at, object).await?;
        }
        Ok(())
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        if cache
            .submit::<D, T>(Some(&*self.db), object_id, event_id, event.clone())
            .await?
        {
            self.db.submit::<T>(object_id, event_id, event).await?;
        }
        Ok(())
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        {
            let cache = self.cache.read().await;
            if let Some(res) = cache.get(&ptr) {
                return Ok(Some(res.clone()));
            }
        }
        let Some(res) = self.db.get::<T>(ptr).await? else {
            return Ok(None);
        };
        debug_assert!(
            res.id() == ptr,
            "Got result with id {:?} instead of expected id {ptr:?}",
            res.id()
        );
        {
            let mut cache = self.cache.write().await;
            cache
                .insert::<T>(res.clone())
                .await
                .with_context(|| format!("inserting object {ptr:?} in the cache"))?;
        }
        Ok(Some(res))
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
        // We cannot use the object cache here, because it is not guaranteed to even
        // contain all the non-heavy objects, due to being an LRU cache. So, immediately
        // delegate to the underlying database, which should forward to either PostgreSQL
        // for the server, or IndexedDB or the API for the client, depending on whether
        // `include_heavy` is set.
        Ok(self
            .db
            .query::<T>(user, include_heavy, ignore_not_modified_on_server_since, q)
            .await?
            .then(|o| async {
                let o = o?;
                let mut cache = self.cache.write().await;
                if let Err(error) = cache.insert::<T>(o.clone()).await {
                    let id = o.id();
                    tracing::error!(?id, ?error, "failed inserting queried object in cache");
                    cache.remove(&id).await;
                }
                Ok(o)
            }))
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        cache.snapshot::<T>(object, time).await?;
        self.db.snapshot::<T>(time, object).await
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        debug_assert!(
            id == hash_binary(&*value),
            "Provided id {id:?} does not match value hash {:?}",
            hash_binary(&*value),
        );
        self.binaries.write().await.insert(id, value.clone());
        self.db.create_binary(id, value).await
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.binaries.read().await.get(&ptr) {
            return Ok(Some(res.clone()));
        }
        let Some(res) = self.db.get_binary(ptr).await? else {
            return Ok(None);
        };
        self.binaries.write().await.insert(ptr, res.clone());
        Ok(Some(res))
    }
}
