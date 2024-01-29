use super::{eq, FullObject};
use crate::{
    db_trait::Db, error::ResultExt, BinPtr, CanDoCallbacks, CrdbStream, DynSized, Event, EventId,
    Object, ObjectId, Query, Timestamp, TypeId, Updatedness, User,
};
use futures::{stream, Stream, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

struct MemDbImpl {
    // Some(e) for a real event, None for a creation snapshot
    events: HashMap<EventId, (ObjectId, Option<Arc<dyn DynSized>>)>,
    // The bool is whether the object is locked, and the set the required_binaries
    objects: HashMap<ObjectId, (TypeId, bool, HashSet<BinPtr>, FullObject)>,
    binaries: HashMap<BinPtr, Arc<[u8]>>,
    is_server: bool,
}

pub struct MemDb(Arc<Mutex<MemDbImpl>>);

impl MemDb {
    pub fn new(is_server: bool) -> MemDb {
        MemDb(Arc::new(Mutex::new(MemDbImpl {
            events: HashMap::new(),
            objects: HashMap::new(),
            binaries: HashMap::new(),
            is_server,
        })))
    }

    pub async fn recreate_all<T: Object>(
        &self,
        event_id: EventId,
        updatedness: Option<Updatedness>,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // disable auto-deref-and-reborrow, get a real mutable borrow
        for (ty, _, required_binaries, o) in this.objects.values_mut() {
            if ty == T::type_ulid() {
                recreate_at::<T>(o, event_id, updatedness, &mut this.events)?;
                *required_binaries = o.required_binaries::<T>();
            }
        }
        Ok(())
    }

    async fn get<T: Object>(&self, lock: bool, object_id: ObjectId) -> crate::Result<FullObject> {
        match self.0.lock().await.objects.get_mut(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _, _, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, locked, _, o)) => {
                *locked |= lock;
                Ok(o.clone())
            }
        }
    }

    pub async fn query<T: Object>(
        &self,
        user: User,
        only_updated_since: Option<Updatedness>,
        q: &Query,
    ) -> crate::Result<Vec<ObjectId>> {
        q.check()?;
        let q = &q;
        let objects = self.0.lock().await.objects.clone(); // avoid deadlock with users_who_can_read below
        let is_server = self.0.lock().await.is_server; // avoid deadlock with users_who_can_read below
        stream::iter(objects.into_iter())
            .filter_map(|(_, (t, _, _, full_object))| async move {
                if t != *T::type_ulid() {
                    return None;
                }
                if let Some(only_updated_since) = only_updated_since {
                    let last_updated = full_object
                        .last_updated()
                        .expect("Query with only_updated_since.is_some() (ie. server) but one object in database had no Updatedness (ie. client)");
                    if last_updated <= only_updated_since {
                        return None;
                    }
                }
                let o = full_object
                    .last_snapshot::<T>()
                    .expect("type error inside MemDb");
                if (is_server
                    && !o
                        .users_who_can_read(self)
                        .await
                        .unwrap()
                        .iter()
                        .any(|u| *u == user))
                    || !q.matches(&*o).unwrap()
                {
                    return None;
                }
                Some(Ok(full_object.id()))
            })
            .collect::<Vec<crate::Result<ObjectId>>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<ObjectId>>>()
    }

    pub async fn unlock(&self, object_id: ObjectId) -> crate::Result<()> {
        if let Some((_, locked, _, _)) = self.0.lock().await.objects.get_mut(&object_id) {
            *locked = false;
        }
        // Always return Ok, even if there's no object it just means it was already unlocked and vacuumed
        Ok(())
    }

    pub async fn vacuum(&self) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // get a real, splittable borrow
        this.objects.retain(|_, (_, locked, _, _)| *locked);
        this.binaries
            .retain(|b, _| this.objects.values().any(|(_, _, req, _)| req.contains(b)));
        Ok(())
    }
}

fn recreate_at<T: Object>(
    o: &FullObject,
    event_id: EventId,
    updatedness: Option<Updatedness>,
    this_events: &mut HashMap<EventId, (ObjectId, Option<Arc<dyn DynSized>>)>,
) -> crate::Result<()> {
    let mut events_before = o
        .changes_clone()
        .into_iter()
        .map(|(e, _)| e)
        .collect::<HashSet<EventId>>();
    events_before.insert(o.created_at());
    o.recreate_at::<T>(event_id, updatedness)
        .wrap_context("recreating object")?;
    let mut events_after = o
        .changes_clone()
        .into_iter()
        .map(|(e, _)| e)
        .collect::<HashSet<EventId>>();
    events_after.insert(o.created_at());
    // Discard all removed events from self.events too
    for e in events_before.difference(&events_after) {
        this_events.remove(&e);
    }
    // And mark the new "creation event" as a creation event
    this_events.get_mut(&o.created_at()).unwrap().1 = None;
    Ok(())
}

impl Db for MemDb {
    async fn create<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;

        // First, check for duplicates
        if let Some((ty, locked, _, o)) = this.objects.get_mut(&object_id) {
            crate::check_strings(&serde_json::to_value(&*object).unwrap())?;
            let c = o.creation_info();
            if ty != T::type_ulid()
                || created_at != c.created_at
                || !eq::<T>(&*c.creation, &*object as _).unwrap()
            {
                return Err(crate::Error::ObjectAlreadyExists(object_id));
            }
            *locked |= lock;
            return Ok(None);
        }
        if let Some(_) = this.events.get(&created_at) {
            crate::check_strings(&serde_json::to_value(&*object).unwrap())?;
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        // Then, check that the data is correct
        crate::check_strings(&serde_json::to_value(&*object).unwrap())?;

        // Then, check for required binaries
        let required_binaries = object.required_binaries();
        let mut missing_binaries = Vec::new();
        for b in required_binaries.iter() {
            if this.binaries.get(b).is_none() {
                missing_binaries.push(*b);
            }
        }
        if !missing_binaries.is_empty() {
            return Err(crate::Error::MissingBinaries(missing_binaries));
        }

        // This is a new insert, do it
        this.objects.insert(
            object_id,
            (
                *T::type_ulid(),
                lock,
                required_binaries.into_iter().collect(),
                FullObject::new(object_id, updatedness, created_at, object.clone()),
            ),
        );
        this.events.insert(created_at, (object_id, None));

        Ok(Some(object))
    }

    async fn submit<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Option<Updatedness>,
        force_lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;
        match this.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _, _, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, _, _, o)) if o.creation_info().created_at >= event_id => {
                Err(crate::Error::EventTooEarly {
                    object_id,
                    event_id,
                    created_at: o.creation_info().created_at,
                })
            }
            Some((_, _, _, o)) => {
                // First, check for duplicates
                if let Some((o, e)) = this.events.get(&event_id) {
                    crate::check_strings(&serde_json::to_value(&*event).unwrap())?;
                    let Some(e) = e else {
                        // else if creation snapshot
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    };
                    if *o != object_id || !eq::<T::Event>(&**e, &*event as _).unwrap_or(false) {
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    }
                    // Just lock the object if requested
                    this.objects.get_mut(&object_id).unwrap().1 |= force_lock;
                    return Ok(None);
                }

                // Then, check that the data is correct
                crate::check_strings(&serde_json::to_value(&*event).unwrap())?;

                // Then, check for required binaries
                let required_binaries = event.required_binaries();
                let mut missing_binaries = Vec::new();
                for b in required_binaries {
                    if this.binaries.get(&b).is_none() {
                        missing_binaries.push(b);
                    }
                }
                if !missing_binaries.is_empty() {
                    return Err(crate::Error::MissingBinaries(missing_binaries));
                }

                // All is good, we can insert
                o.apply::<T>(event_id, event.clone(), updatedness)?;
                let last_snapshot = o.last_snapshot::<T>().unwrap();
                this.objects.get_mut(&object_id).unwrap().2 = o.required_binaries::<T>();
                this.objects.get_mut(&object_id).unwrap().1 |= force_lock;
                this.events.insert(event_id, (object_id, Some(event)));
                Ok(Some(last_snapshot))
            }
        }
    }

    async fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        let res = self.get::<T>(lock, object_id).await?;
        res.last_snapshot::<T>()
            .wrap_context("retrieving last snapshot")
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        force_lock: bool,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;

        // First, check for preconditions
        let Some(&(real_type_id, _, _, ref o)) = this.objects.get(&object_id) else {
            std::mem::drop(this);
            return self
                .create(object_id, new_created_at, object, updatedness, force_lock)
                .await;
        };
        if real_type_id != *T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id,
            });
        }
        if o.created_at() > new_created_at {
            return Err(crate::Error::EventTooEarly {
                event_id: new_created_at,
                object_id,
                created_at: o.created_at(),
            });
        }
        if let Some(e) = this.events.get(&new_created_at) {
            if e.0 != object_id {
                crate::check_strings(&serde_json::to_value(&*object).unwrap())?;
                return Err(crate::Error::EventAlreadyExists(new_created_at));
            }
        }

        // Then, check that the data is correct
        crate::check_strings(&serde_json::to_value(&*object).unwrap())?;

        // Then, check for required binaries
        let required_binaries = object.required_binaries();
        let mut missing_binaries = Vec::new();
        for b in required_binaries {
            if this.binaries.get(&b).is_none() {
                missing_binaries.push(b);
            }
        }
        if !missing_binaries.is_empty() {
            return Err(crate::Error::MissingBinaries(missing_binaries));
        }

        // All good, do the recreation
        o.recreate_with::<T>(new_created_at, object, updatedness);
        let required_binaries = o.required_binaries::<T>();
        let last_snapshot = o.last_snapshot::<T>().unwrap();
        let this_object = this.objects.get_mut(&object_id).unwrap();
        this_object.1 |= force_lock;
        this_object.2 = required_binaries;

        Ok(Some(last_snapshot))
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        self.0.lock().await.objects.remove(&object_id);
        Ok(())
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if binary_id != crate::hash_binary(&data) {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        self.0.lock().await.binaries.insert(binary_id, data);
        Ok(())
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        Ok(self.0.lock().await.binaries.get(&binary_id).cloned())
    }
}
