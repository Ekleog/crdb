use crate::{error::ResultExt, full_object::FullObject, EventId, Object, ObjectId, Timestamp};
use anyhow::{anyhow, Context};
use std::{
    collections::{btree_map, hash_map, BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct ObjectCache {
    watermark: usize,
    // `Instant` here is the last access time
    objects: HashMap<ObjectId, (Instant, FullObject)>,
    last_accessed: BTreeMap<Instant, Vec<ObjectId>>,
    size: usize,
}

struct SizeUpdater<'a> {
    object: &'a FullObject,
    size: &'a mut usize,
}

impl<'a> SizeUpdater<'a> {
    fn new(size: &'a mut usize, object: &'a FullObject) -> SizeUpdater<'a> {
        *size -= object.deep_size_of();
        SizeUpdater { object, size }
    }
}

impl<'a> Drop for SizeUpdater<'a> {
    fn drop(&mut self) {
        *self.size += self.object.deep_size_of();
    }
}

impl ObjectCache {
    pub fn new(watermark: usize) -> ObjectCache {
        ObjectCache {
            watermark,
            objects: HashMap::new(),
            last_accessed: BTreeMap::new(),
            size: 0,
        }
    }

    fn create_impl<T: Object>(
        &mut self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<bool> {
        let cache_entry = self.objects.entry(object_id);
        match cache_entry {
            hash_map::Entry::Occupied(mut entry) => {
                let (t, o) = entry.get_mut();
                let o = o.creation_info();
                if o.created_at != created_at
                    || o.id != object_id
                    || o.creation
                        .ref_to_any()
                        .downcast_ref::<T>()
                        .map(|v| v != &*object)
                        .unwrap_or(true)
                {
                    return Err(crate::Error::ObjectAlreadyExists(object_id));
                }
                std::mem::drop(o);
                *t = Self::touched(&mut self.last_accessed, object_id, *t);
                Ok(false)
            }
            hash_map::Entry::Vacant(v) => {
                let o = FullObject::new(object_id, created_at, object);
                self.size += o.deep_size_of();
                let t = Self::created(&mut self.last_accessed, object_id);
                v.insert((t, o));
                Ok(true)
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
            panic!("Called `ObjectCache::removed` with wrong `previous_time`");
        };
        let v = e.get_mut();
        v.swap_remove(v.iter().position(|x| x == &id).expect(
            "Called `ObjectCache::removed` with an `id` that does not match `previous_time`",
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
    pub fn create<T: Object>(
        &mut self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> crate::Result<bool> {
        let res = self.create_impl(id, created_at, object);
        self.apply_watermark();
        res
    }

    pub fn remove(&mut self, object_id: &ObjectId) {
        if let Some((t, o)) = self.objects.remove(object_id) {
            Self::removed(&mut self.last_accessed, *object_id, t);
            self.size -= o.deep_size_of();
            self.apply_watermark();
        }
    }

    /// Returns `true` if the event was previously absent from the cache, and `false` if
    /// the event was already present in the cache. Returns an error if another event
    /// with the same id had already been applied, if the event is earlier than the
    /// object's last recreation time, if the provided `T` is wrong or if the database
    /// failed to return the pre-event object.
    ///
    /// If `db` is `Some`, then this will automatically fetch the contents for `object_id`
    /// if it is not in the cache yet.
    pub fn submit<T: Object>(
        &mut self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> crate::Result<bool> {
        let Some((t, object)) = self.objects.get_mut(&object_id) else {
            self.apply_watermark();
            return Ok(true); // Object was absent
        };
        let updater = SizeUpdater::new(&mut self.size, &object);
        let res = object
            .apply::<T>(event_id, event)
            .wrap_with_context(|| format!("applying {event_id:?} on {object_id:?}"))?;
        *t = Self::touched(&mut self.last_accessed, object_id, *t);
        std::mem::drop(updater);
        self.apply_watermark();
        Ok(res)
    }

    pub fn recreate<T: Object>(&mut self, object: ObjectId, time: Timestamp) -> crate::Result<()> {
        if let Some((t, o)) = self.objects.get_mut(&object) {
            let updater = SizeUpdater::new(&mut self.size, &o);
            o.recreate_at::<T>(time)?;
            *t = Self::touched(&mut self.last_accessed, object, *t);
            std::mem::drop(updater);
            self.apply_watermark();
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

    pub fn insert<T: Object>(&mut self, o: FullObject) -> anyhow::Result<()> {
        let (creation, changes) = o.extract_all_clone();
        // Do not directly insert into the hashmap, because the hashmap could already contain more
        // recent events for this object. Instead, pass the object and all the events one by one,
        // to merge with anything that would already exist.
        self.create_impl(
            creation.id,
            creation.created_at,
            creation
                .creation
                .arc_to_any()
                .downcast::<T>()
                .map_err(|_| anyhow!("Failed to downcast an object to {:?}", T::type_ulid()))?,
        )
        .with_context(|| format!("creating object {:?}", creation.id))?;
        let (t, created) = self
            .objects
            .get(&creation.id)
            .expect("Failed to retrieve an object just created");
        let size_updater = SizeUpdater::new(&mut self.size, &created);
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
                                T::type_ulid()
                            )
                        })?,
                )
                .with_context(|| format!("applying {event_id:?} on {:?}", creation.id))?;
        }
        std::mem::drop(size_updater);
        self.objects.get_mut(&creation.id).unwrap().0 =
            Self::touched(&mut self.last_accessed, creation.id, *t);
        self.apply_watermark();
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

    /// The average size of objects in the hashmap. Do not call on an empty hashmap!
    ///
    /// This function's contract includes never returning 0
    fn average_size(&self) -> usize {
        let num = self.objects.len();
        (self.size + num - 1) / num
    }

    fn apply_watermark(&mut self) {
        if let Some(max_size_removed) = self.size.checked_sub(self.watermark) {
            if max_size_removed == 0 {
                return;
            }
            let max_items_checked = 3 * max_size_removed / self.average_size();
            let original_num = self.objects.len();
            let original_size = self.size;
            self.reduce_size(max_items_checked, max_size_removed);
            // If we went through all the objects without removing anything, increase the watermark
            if max_items_checked == original_num && self.size == original_size {
                self.watermark = self.size * 10 / 8; // Add some leeway to avoid this happening too often
            }
        }
    }

    pub fn reduce_size_to(&mut self, size: usize) {
        if let Some(s) = self.size.checked_sub(size) {
            self.reduce_size(self.objects.len(), s);
        }
    }

    pub fn reduce_size(&mut self, max_items_checked: usize, max_size_removed: usize) {
        let max_items_checked = std::cmp::min(max_items_checked, self.objects.len());
        let mut max_size_removed = std::cmp::min(max_size_removed, self.size);
        for _ in 0..max_items_checked {
            if max_size_removed == 0 {
                return;
            }

            // Retrieve the first ID to check
            let Some(mut accessed_entry) = self.last_accessed.first_entry() else {
                return;
            };
            let t = *accessed_entry.key();
            let id = accessed_entry
                .get_mut()
                .pop()
                .expect("Empty Vec in ObjectCache's last_accessed");
            if accessed_entry.get().is_empty() {
                accessed_entry.remove();
            }

            // Remove it if the object has no refcount, update last update time if not
            let hash_map::Entry::Occupied(mut object_entry) = self.objects.entry(id) else {
                panic!("`ObjectCache`'s `last_accessed` contained an invalid reference");
            };
            debug_assert!(
                object_entry.get().0 == t,
                "`ObjectCache`'s last accessed time disagrees with `last_accessed`'s view: {t:?} != {:?}",
                object_entry.get().0,
            );
            if object_entry.get().1.refcount() == 1 {
                let s = object_entry.get().1.deep_size_of();
                self.size -= s;
                max_size_removed = max_size_removed.saturating_sub(s);
                object_entry.remove();
            } else {
                let t = Self::created(&mut self.last_accessed, object_entry.get().1.id());
                object_entry.get_mut().0 = t;
            }
        }
    }

    #[cfg(test)]
    fn assert_invariants(&self, at: impl Fn() -> String) {
        let mut total_size = 0;
        for (id, (t, o)) in self.objects.iter() {
            total_size += o.deep_size_of();
            self.last_accessed
                .get(t)
                .unwrap_or_else(|| panic!("full last_accessed dump: {:?},\ngetting ids for present-in-objects `t` ({t:?})\nexpecting at least {id:?}\n-- at {}", self.last_accessed, at()))
                .iter()
                .find(|v| v == &id)
                .unwrap_or_else(|| panic!("having id in the ids at t -- at {}", at()));
        }
        assert_eq!(total_size, self.size, "size mismatch -- at {}", at());
        for (t, ids) in self.last_accessed.iter() {
            for id in ids.iter() {
                let o = self
                    .objects
                    .get(id)
                    .unwrap_or_else(|| panic!("getting object at id -- at {}", at()));
                assert_eq!(t, &o.0, "time mismatch -- at {}", at());
                assert_eq!(&o.1.id(), id, "id mismatch -- at {}", at());
            }
        }
    }
}
