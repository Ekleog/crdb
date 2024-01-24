use super::eq;
use crate::{
    db_trait::Db,
    error::ResultExt,
    full_object::{DynSized, FullObject},
    BinPtr, CanDoCallbacks, CrdbStream, Event, EventId, Object, ObjectId, Query, Timestamp, TypeId,
    User,
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
    objects: HashMap<ObjectId, (TypeId, FullObject)>,
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

    pub async fn recreate_all<T: Object>(&self, time: Timestamp) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // disable auto-deref-and-reborrow, get a real mutable borrow
        EventId::last_id_at(time)?;
        for (ty, o) in this.objects.values() {
            if ty == T::type_ulid() {
                recreate::<T>(o, time, &mut this.events)?;
            }
        }
        Ok(())
    }

    pub async fn query<T: Object>(
        &self,
        user: User,
        only_updated_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<Vec<ObjectId>> {
        assert!(
            // TODO(test): with the new Db api this should get better
            only_updated_since.is_none(),
            "Time-based tests are currently not implemented"
        );
        q.check()?;
        let q = &q;
        let objects = self.0.lock().await.objects.clone(); // avoid deadlock with users_who_can_read below
        let is_server = self.0.lock().await.is_server; // avoid deadlock with users_who_can_read below
        stream::iter(objects.into_iter())
            .filter_map(|(_, (t, full_object))| async move {
                if t != *T::type_ulid() {
                    return None;
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

    pub async fn unlock(&self, _object_id: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(test)
    }
}

fn recreate<T: Object>(
    o: &FullObject,
    time: Timestamp,
    this_events: &mut HashMap<EventId, (ObjectId, Option<Arc<dyn DynSized>>)>,
) -> crate::Result<()> {
    let mut events_before = o
        .changes_clone()
        .into_iter()
        .map(|(e, _)| e)
        .collect::<HashSet<EventId>>();
    events_before.insert(o.created_at());
    o.recreate_at::<T>(time).wrap_context("recreating object")?;
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
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        _lock: bool, // TODO(test): implement (un)lock semantics for client-side tests
        _cb: &C,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;

        // First, check for duplicates
        if let Some((ty, o)) = this.objects.get(&object_id) {
            crate::check_strings(&serde_json::to_value(&*object).unwrap())?;
            let c = o.creation_info();
            if ty != T::type_ulid()
                || created_at != c.created_at
                || !eq::<T>(&*c.creation, &*object as _).unwrap()
            {
                return Err(crate::Error::ObjectAlreadyExists(object_id));
            }
            return Ok(());
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
        for b in required_binaries {
            if this.binaries.get(&b).is_none() {
                missing_binaries.push(b);
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
                FullObject::new(object_id, created_at, object),
            ),
        );
        this.events.insert(created_at, (object_id, None));

        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        match this.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, o)) if o.creation_info().created_at >= event_id => {
                Err(crate::Error::EventTooEarly {
                    object_id,
                    event_id,
                    created_at: o.creation_info().created_at,
                })
            }
            Some((_, o)) => {
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
                    return Ok(());
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
                o.apply::<T>(event_id, event.clone())?;
                this.events.insert(event_id, (object_id, Some(event)));
                Ok(())
            }
        }
    }

    async fn get<T: Object>(&self, _lock: bool, object_id: ObjectId) -> crate::Result<FullObject> {
        match self.0.lock().await.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, o)) => Ok(o.clone()),
        }
    }

    async fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        // TODO(high): actually implement properly
        let res = Db::get::<T>(self, lock, object_id).await?;
        res.last_snapshot::<T>()
            .wrap_context("retrieving last snapshot")
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        time: Timestamp,
        _cb: &C,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // get a real borrow and not a RefMut struct
        EventId::last_id_at(time)?; // start by checking the timestamp
        let Some((ty, o)) = this.objects.get(&object_id) else {
            return Err(crate::Error::ObjectDoesNotExist(object_id));
        };
        if ty != T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            });
        }
        recreate::<T>(o, time, &mut this.events).wrap_context("recreating object")?;
        Ok(())
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        unimplemented!() // TODO(test)
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if binary_id != crate::hash_binary(&data) {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        self.0.lock().await.binaries.insert(binary_id, data);
        Ok(())
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<[u8]>>> {
        Ok(self.0.lock().await.binaries.get(&binary_id).cloned())
    }
}
