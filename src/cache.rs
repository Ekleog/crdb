use crate::{
    api::{BinPtr, Query},
    db_trait::{Db, EventId, FullObject, NewEvent, NewObject, NewSnapshot, ObjectId, Timestamp},
    hash_binary, Object, User,
};
use anyhow::{anyhow, Context};
use futures::{pin_mut, Stream, StreamExt};
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    future::Future,
    sync::Arc,
};
use tokio::sync::RwLock;

#[doc(hidden)]
#[derive(Clone)]
pub struct ObjectCache {
    objects: HashMap<ObjectId, FullObject>,
}

impl ObjectCache {
    fn new() -> ObjectCache {
        ObjectCache {
            objects: HashMap::new(),
        }
    }

    fn create_impl<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<(bool, &mut FullObject)> {
        let cache_entry = self.objects.entry(id);
        match cache_entry {
            hash_map::Entry::Occupied(entry) => {
                let o = entry.get();
                anyhow::ensure!(
                    o.created_at == created_at
                        && o.id == id
                        && o.creation
                            .clone()
                            .downcast::<T>()
                            .map(|v| v == object)
                            .unwrap_or(false),
                    "Object {id:?} was already created with a different initial value"
                );
                Ok((false, entry.into_mut()))
            }
            hash_map::Entry::Vacant(v) => {
                let res = v.insert(FullObject {
                    id,
                    created_at,
                    creation: object,
                    changes: Arc::new(RwLock::new(BTreeMap::new())),
                });
                Ok((true, res))
            }
        }
    }

    /// Returns `true` if the object was newly inserted in the cache, and `false` if
    /// the object was already present in the cache. Errors if the object id was already
    /// in the cache with a different value.
    pub fn create<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<bool> {
        self.create_impl(id, created_at, object).map(|r| r.0)
    }

    fn remove(&mut self, object_id: &ObjectId) {
        self.objects.remove(object_id);
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
            hash_map::Entry::Occupied(mut object) => object
                .get_mut()
                .apply::<T>(event_id, event)
                .await
                .with_context(|| format!("applying {event_id:?} on {object_id:?}")),
            hash_map::Entry::Vacant(v) => {
                if let Some(db) = db {
                    let o = db
                        .get::<T>(object_id)
                        .await
                        .with_context(|| format!("getting {object_id:?} from database"))?
                        .ok_or_else(|| anyhow!("Submitted an event to object {object_id:?} that does not exist in the db"))?;
                    let o = v.insert(o);
                    o.apply::<T>(event_id, event)
                        .await
                        .with_context(|| format!("applying {event_id:?} on {object_id:?}"))
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
            o.recreate_at::<T>(time).await?;
        }
        Ok(())
    }

    fn get(&self, id: &ObjectId) -> Option<&FullObject> {
        self.objects.get(id)
    }

    async fn insert<T: Object>(
        &mut self,
        object_id: ObjectId,
        o: FullObject,
    ) -> anyhow::Result<()> {
        debug_assert!(object_id == o.id, "inserting an object with wrong id");
        // Do not directly insert into the hashmap, because the hashmap could already contain more
        // recent events for this object. Instead, pass the object and all the events one by one,
        // to merge with anything that would already exist.
        let (_, created) = self
            .create_impl(
                o.id,
                o.created_at,
                o.creation
                    .downcast::<T>()
                    .map_err(|_| anyhow!("Failed to downcast an object to {:?}", T::ulid()))?,
            )
            .with_context(|| format!("creating object {object_id:?}"))?;
        for (event_id, c) in o.changes.read().await.iter() {
            created
                .apply::<T>(
                    *event_id,
                    c.event.clone().downcast::<T::Event>().map_err(|_| {
                        anyhow!(
                            "Failed to downcast an event to {:?}'s event type",
                            T::ulid()
                        )
                    })?,
                )
                .await
                .with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
        }
        Ok(())
    }
}

#[doc(hidden)]
pub trait CacheConfig {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `create` method with the proper type and the fields from `o`.
    fn create(cache: &mut ObjectCache, o: NewObject) -> anyhow::Result<bool>;

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
}

pub(crate) struct Cache<D: Db> {
    db: Arc<D>,
    // TODO: figure out how to purge from cache (LRU-style), using DeepSizeOf
    cache: Arc<RwLock<ObjectCache>>,
    binaries: Arc<RwLock<HashMap<BinPtr, Arc<Vec<u8>>>>>,
}

impl<D: Db> Cache<D> {
    pub(crate) fn new<C: CacheConfig>(db: Arc<D>) -> Cache<D> {
        let cache = Arc::new(RwLock::new(ObjectCache::new()));

        // Watch new objects
        tokio::task::spawn({
            let db = db.clone();
            let cache = cache.clone();
            async move {
                let new_objects = db.new_objects().await;
                pin_mut!(new_objects);
                while let Some(o) = new_objects.next().await {
                    let mut cache = cache.write().await;
                    let object = o.id;
                    if let Err(error) = C::create(&mut *cache, o) {
                        tracing::error!(?error, ?object, "failed creating received object");
                    }
                }
            }
        });

        // Watch new events
        tokio::task::spawn({
            let db = db.clone();
            let cache = cache.clone();
            async move {
                let new_events = db.new_events().await;
                pin_mut!(new_events);
                while let Some(e) = new_events.next().await {
                    let mut cache = cache.write().await;
                    let object = e.object_id;
                    let event = e.id;
                    // DO NOT re-fetch object when receiving an event not in cache for it.
                    // Without this, users would risk unsubscribing from an object, then receiving
                    // an event on this object (as a race condition), and then staying subscribed.
                    if let Err(error) = C::submit::<D>(None, &mut *cache, e).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            ?event,
                            "failed submitting received event"
                        );
                    }
                }
            }
        });

        // Watch new snapshots
        tokio::task::spawn({
            let db = db.clone();
            let cache = cache.clone();
            async move {
                let new_snapshots = db.new_snapshots().await;
                pin_mut!(new_snapshots);
                while let Some(s) = new_snapshots.next().await {
                    let mut cache = cache.write().await;
                    let object = s.object_id;
                    if let Err(error) = C::snapshot(&mut *cache, s).await {
                        tracing::error!(
                            ?error,
                            ?object,
                            "failed snapshotting as per received event"
                        )
                    }
                }
            }
        });

        // Return the cache
        Self {
            db,
            cache,
            binaries: Arc::new(RwLock::new(HashMap::new())),
        }
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
        cache.remove(&ptr);
        self.db.unsubscribe(ptr).await
    }

    async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<()> {
        let mut cache = self.cache.write().await;
        if cache.create(id, created_at, object.clone())? {
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
        {
            let mut cache = self.cache.write().await;
            cache
                .insert::<T>(ptr, res.clone())
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
                if let Err(error) = cache.insert::<T>(o.id, o.clone()).await {
                    tracing::error!(id = ?o.id, ?error, "failed inserting queried object in cache");
                    cache.remove(&o.id);
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
        let mut binaries = self.binaries.write().await;
        binaries.insert(id, value.clone());
        self.db.create_binary(id, value).await
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        {
            let binaries = self.binaries.read().await;
            if let Some(res) = binaries.get(&ptr) {
                return Ok(Some(res.clone()));
            }
        }
        let Some(res) = self.db.get_binary(ptr).await? else {
            return Ok(None);
        };
        {
            let mut binaries = self.binaries.write().await;
            binaries.insert(ptr, res.clone());
        }
        Ok(Some(res))
    }
}
