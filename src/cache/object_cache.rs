use crate::{
    db_trait::{Db, EventId, ObjectId},
    full_object::FullObject,
    Object, Timestamp,
};
use anyhow::{anyhow, Context};
use std::{
    collections::{btree_map, hash_map, BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

#[derive(Clone)]
pub struct ObjectCache {
    // `Instant` here is the last access time
    objects: HashMap<ObjectId, (Instant, FullObject)>,
    last_accessed: BTreeMap<Instant, Vec<ObjectId>>,
    size: usize,
    // TODO: have fuzzers that assert that `size` stays in-sync with `objects`, as well as `last_accessed`
}

impl ObjectCache {
    pub fn new() -> ObjectCache {
        ObjectCache {
            objects: HashMap::new(),
            last_accessed: BTreeMap::new(),
            size: 0,
        }
    }

    async fn create_impl<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> anyhow::Result<(bool, &mut FullObject, Instant)> {
        let cache_entry = self.objects.entry(id);
        match cache_entry {
            hash_map::Entry::Occupied(entry) => {
                let (t, o) = entry.get();
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
                let t = Self::touched(&mut self.last_accessed, id, *t);
                Ok((false, &mut entry.into_mut().1, t))
            }
            hash_map::Entry::Vacant(v) => {
                let o = FullObject::new(id, created_at, object);
                self.size += o.deep_size_of();
                let t = Self::created(&mut self.last_accessed, id);
                let res = v.insert((t, o));
                Ok((true, &mut res.1, t))
            }
        }
    }

    fn created(last_accessed: &mut BTreeMap<Instant, Vec<ObjectId>>, id: ObjectId) -> Instant {
        let now = Instant::now();
        last_accessed.entry(now).or_insert_with(Vec::new).push(id);
        now
    }

    fn removed(
        last_accessed: &mut BTreeMap<Instant, Vec<ObjectId>>,
        id: ObjectId,
        previous_time: Instant,
    ) {
        let btree_map::Entry::Occupied(mut e) = last_accessed.entry(previous_time) else {
            panic!("Called `ObjectCache::touched` with wrong `previous_time`");
        };
        let v = e.get_mut();
        v.swap_remove(v.iter().position(|x| x == &id).expect(
            "Called `ObjectCache::touched` with an `id` that does not match `previous_time`",
        ));
        if v.is_empty() {
            e.remove();
        }
    }

    fn touched(
        last_accessed: &mut BTreeMap<Instant, Vec<ObjectId>>,
        id: ObjectId,
        previous_time: Instant,
    ) -> Instant {
        Self::removed(last_accessed, id, previous_time);
        Self::created(last_accessed, id)
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

    pub async fn remove(&mut self, object_id: &ObjectId) {
        if let Some((t, o)) = self.objects.remove(object_id) {
            Self::removed(&mut self.last_accessed, *object_id, t);
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
                let (t, object) = object.get();
                self.size -= object.deep_size_of();
                let res = object
                    .apply::<T>(event_id, event)
                    .with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
                self.size += object.deep_size_of();
                Self::touched(&mut self.last_accessed, object_id, *t);
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
                    let t = Self::created(&mut self.last_accessed, object_id);
                    v.insert((t, o));
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
        if let Some((t, o)) = self.objects.get_mut(&object) {
            self.size -= o.deep_size_of();
            o.recreate_at::<T>(time)?;
            self.size += o.deep_size_of();
            Self::touched(&mut self.last_accessed, object, *t);
        }
        Ok(())
    }

    pub fn get(&self, id: &ObjectId) -> Option<&FullObject> {
        // Note: we do not actually remember that `id` was touched. This is not too bad, because
        // long-lived `Arc`s during the cache cleanup will lead to them being marked as used.
        // TODO: still, we should probably record that `id` was touched somewhere, to deal with
        // short-term but recurrent read accesses.
        self.objects.get(id).map(|v| &v.1)
    }

    pub async fn insert<T: Object>(&mut self, o: FullObject) -> anyhow::Result<()> {
        let (creation, changes) = o.extract_all_clone();
        // Do not directly insert into the hashmap, because the hashmap could already contain more
        // recent events for this object. Instead, pass the object and all the events one by one,
        // to merge with anything that would already exist.
        let (_, created, t) = self
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
        Self::touched(&mut self.last_accessed, creation.id, t);
        Ok(())
    }

    pub fn clear(&mut self) {
        self.objects.retain(|_, (t, o)| {
            if o.refcount() == 1 {
                self.size -= o.deep_size_of();
                Self::removed(&mut self.last_accessed, o.id(), *t);
                false
            } else {
                true
            }
        })
    }
}