use super::{eq, FullObject};
use crdb_core::{
    BinPtr, ClientSideDb, ClientStorageInfo, CrdbSyncFn, Db, DynSized, Event, EventId, Importance,
    LoginInfo, Object, ObjectId, Query, QueryId, ResultExt, SavedObjectMeta, SavedQuery, TypeId,
    Updatedness, Upload, UploadId, User,
};
use futures::{stream, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

type MemDbEvents = HashMap<EventId, (ObjectId, Option<Arc<dyn DynSized>>)>;

struct MemDbImpl {
    // Some(e) for a real event, None for a creation snapshot
    events: MemDbEvents,
    // The set is the list of required_binaries
    objects: HashMap<ObjectId, MemDbObj>,
    binaries: HashMap<BinPtr, Arc<[u8]>>,
    is_server: bool,
}

#[derive(Clone)]
struct MemDbObj {
    type_id: TypeId,
    importance: Importance,
    required_binaries: HashSet<BinPtr>,
    data: FullObject,
    importance_from_queries: Importance,
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
        for o in this.objects.values_mut() {
            if o.type_id == *T::type_ulid() {
                recreate_at::<T>(&o.data, event_id, updatedness, &mut this.events)?;
                o.required_binaries = o.data.required_binaries::<T>();
            }
        }
        Ok(())
    }

    async fn get<T: Object>(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> crate::Result<FullObject> {
        match self.0.lock().await.objects.get_mut(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some(o) if o.type_id != *T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: o.type_id,
            }),
            Some(o) => {
                o.importance |= importance;
                Ok(o.data.clone())
            }
        }
    }

    pub async fn memdb_query<T: Object>(
        &self,
        user: User,
        only_updated_since: Option<Updatedness>,
        query: &Query,
    ) -> crate::Result<Vec<ObjectId>> {
        query.check()?;
        let objects = self.0.lock().await.objects.clone(); // avoid deadlock with users_who_can_read below
        let is_server = self.0.lock().await.is_server; // avoid deadlock with users_who_can_read below
        stream::iter(objects.into_iter())
            .filter_map(|(_, o)| async move {
                if o.type_id != *T::type_ulid() {
                    return None;
                }
                if let Some(only_updated_since) = only_updated_since {
                    let last_updated = o.data
                        .last_updated()
                        .expect("Query with only_updated_since.is_some() (ie. server) but one object in database had no Updatedness (ie. client)");
                    if last_updated <= only_updated_since {
                        return None;
                    }
                }
                let data = o.data
                    .last_snapshot::<T>()
                    .expect("type error inside MemDb");
                if (is_server
                    && !data
                        .users_who_can_read(self)
                        .await
                        .unwrap()
                        .iter()
                        .any(|u| *u == user))
                    || !query.matches(&*data).unwrap()
                {
                    return None;
                }
                Some(Ok(o.data.id()))
            })
            .collect::<Vec<crate::Result<ObjectId>>>()
            .await
            .into_iter()
            .collect::<crate::Result<Vec<ObjectId>>>()
    }
}

fn recreate_at<T: Object>(
    o: &FullObject,
    event_id: EventId,
    updatedness: Option<Updatedness>,
    this_events: &mut MemDbEvents,
) -> crate::Result<()> {
    let mut events_before = o.changes_clone().into_keys().collect::<HashSet<EventId>>();
    events_before.insert(o.created_at());
    o.recreate_at::<T>(event_id, updatedness)
        .wrap_context("recreating object")?;
    let mut events_after = o.changes_clone().into_keys().collect::<HashSet<EventId>>();
    events_after.insert(o.created_at());
    // Discard all removed events from self.events too
    for e in events_before.difference(&events_after) {
        this_events.remove(e);
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
        importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;

        // First, check for duplicates
        if let Some(o) = this.objects.get_mut(&object_id) {
            crdb_core::check_strings(&serde_json::to_value(&*object).unwrap())?;
            let c = o.data.creation_info();
            if o.type_id != *T::type_ulid()
                || created_at != c.created_at
                || !eq::<T>(&*c.creation, &*object as _).unwrap()
            {
                return Err(crate::Error::ObjectAlreadyExists(object_id));
            }
            o.importance |= importance;
            return Ok(None);
        }
        if this.events.contains_key(&created_at) {
            crdb_core::check_strings(&serde_json::to_value(&*object).unwrap())?;
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        // Then, check that the data is correct
        crdb_core::check_strings(&serde_json::to_value(&*object).unwrap())?;

        // Then, check for required binaries
        let required_binaries = object.required_binaries();
        let mut missing_binaries = Vec::new();
        for b in required_binaries.iter() {
            if !this.binaries.contains_key(b) {
                missing_binaries.push(*b);
            }
        }
        if !missing_binaries.is_empty() {
            return Err(crate::Error::MissingBinaries(missing_binaries));
        }

        // This is a new insert, do it
        this.objects.insert(
            object_id,
            MemDbObj {
                type_id: *T::type_ulid(),
                importance,
                required_binaries: required_binaries.into_iter().collect(),
                data: FullObject::new(object_id, updatedness, created_at, object.clone()),
                importance_from_queries: Importance::NONE,
            },
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
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;
        match this.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some(o) if o.type_id != *T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: o.type_id,
            }),
            Some(o) if o.data.creation_info().created_at >= event_id => {
                Err(crate::Error::EventTooEarly {
                    object_id,
                    event_id,
                    created_at: o.data.creation_info().created_at,
                })
            }
            Some(o) => {
                // First, check for duplicates
                if let Some((prev_object_id, e)) = this.events.get(&event_id) {
                    crdb_core::check_strings(&serde_json::to_value(&*event).unwrap())?;
                    let Some(e) = e else {
                        // else if creation snapshot
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    };
                    if *prev_object_id != object_id
                        || !eq::<T::Event>(&**e, &*event as _).unwrap_or(false)
                    {
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    }
                    // Just lock the object if requested
                    this.objects.get_mut(&object_id).unwrap().importance |= additional_importance;
                    return Ok(None);
                }

                // Then, check that the data is correct
                crdb_core::check_strings(&serde_json::to_value(&*event).unwrap())?;

                // Then, check for required binaries
                let required_binaries = event.required_binaries();
                let mut missing_binaries = Vec::new();
                for b in required_binaries {
                    if !this.binaries.contains_key(&b) {
                        missing_binaries.push(b);
                    }
                }
                if !missing_binaries.is_empty() {
                    return Err(crate::Error::MissingBinaries(missing_binaries));
                }

                // All is good, we can insert
                o.data.apply::<T>(event_id, event.clone(), updatedness)?;
                let last_snapshot = o.data.last_snapshot::<T>().unwrap();
                this.objects.get_mut(&object_id).unwrap().required_binaries =
                    o.data.required_binaries::<T>();
                this.objects.get_mut(&object_id).unwrap().importance |= additional_importance;
                this.events.insert(event_id, (object_id, Some(event)));
                Ok(Some(last_snapshot))
            }
        }
    }

    async fn get_latest<T: Object>(
        &self,
        object_id: ObjectId,
        importance: Importance,
    ) -> crate::Result<Arc<T>> {
        let res = self.get::<T>(object_id, importance).await?;
        res.last_snapshot::<T>()
            .wrap_context("retrieving last snapshot")
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if binary_id != crdb_core::hash_binary(&data) {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        self.0.lock().await.binaries.insert(binary_id, data);
        Ok(())
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        Ok(self.0.lock().await.binaries.get(&binary_id).cloned())
    }

    async fn reencode_old_versions<T: Object>(&self) -> usize {
        unimplemented!() // TODO(test-med): should test it with the to-do multiple-versions object type
    }

    async fn assert_invariants_generic(&self) {
        // Always ok
    }

    async fn assert_invariants_for<T: Object>(&self) {
        // Always ok
    }
}

impl ClientSideDb for MemDb {
    async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        unimplemented!() // TODO(test-high)
    }

    async fn save_login(&self, _info: LoginInfo) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }

    async fn get_saved_login(&self) -> crate::Result<Option<LoginInfo>> {
        unimplemented!() // TODO(test-high)
    }

    async fn get_json(
        &self,
        _object_id: ObjectId,
        _importance: Importance,
    ) -> crate::Result<serde_json::Value> {
        unimplemented!()
    }

    async fn remove_everything(&self) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }

    async fn recreate<T: Object>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        object: Arc<T>,
        updatedness: Option<Updatedness>,
        additional_importance: Importance,
    ) -> crate::Result<Option<Arc<T>>> {
        let mut this = self.0.lock().await;

        // First, check for preconditions
        let Some(o) = this.objects.get(&object_id) else {
            std::mem::drop(this);
            return self
                .create(
                    object_id,
                    new_created_at,
                    object,
                    updatedness,
                    additional_importance,
                )
                .await;
        };
        if o.type_id != *T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: o.type_id,
            });
        }
        if o.data.created_at() > new_created_at {
            return Err(crate::Error::EventTooEarly {
                event_id: new_created_at,
                object_id,
                created_at: o.data.created_at(),
            });
        }
        if let Some((prev_object_id, _)) = this.events.get(&new_created_at) {
            if *prev_object_id != object_id {
                crdb_core::check_strings(&serde_json::to_value(&*object).unwrap())?;
                return Err(crate::Error::EventAlreadyExists(new_created_at));
            }
        }

        // Then, check that the data is correct
        crdb_core::check_strings(&serde_json::to_value(&*object).unwrap())?;

        // Then, check for required binaries
        let required_binaries = object.required_binaries();
        let mut missing_binaries = Vec::new();
        for b in required_binaries {
            if !this.binaries.contains_key(&b) {
                missing_binaries.push(b);
            }
        }
        if !missing_binaries.is_empty() {
            return Err(crate::Error::MissingBinaries(missing_binaries));
        }

        // All good, do the recreation
        o.data
            .recreate_with::<T>(new_created_at, object, updatedness);
        let required_binaries = o.data.required_binaries::<T>();
        let last_snapshot = o.data.last_snapshot::<T>().unwrap();
        let this_object = this.objects.get_mut(&object_id).unwrap();
        this_object.importance |= additional_importance;
        this_object.required_binaries = required_binaries;

        Ok(Some(last_snapshot))
    }

    async fn client_query(
        &self,
        _type_id: TypeId,
        _query: Arc<Query>,
    ) -> crate::Result<Vec<ObjectId>> {
        unimplemented!("See memdb_query")
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        self.0.lock().await.objects.remove(&object_id);
        Ok(())
    }

    async fn remove_event<T: Object>(
        &self,
        _object_id: ObjectId,
        _event_id: EventId,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }

    async fn set_object_importance(
        &self,
        object_id: ObjectId,
        new_importance: Importance,
    ) -> crate::Result<()> {
        if let Some(o) = self.0.lock().await.objects.get_mut(&object_id) {
            o.importance = new_importance;
        }
        // Always return Ok, even if there's no object it just means it was already unlocked and vacuumed
        Ok(())
    }

    async fn set_importance_from_queries(
        &self,
        object_id: ObjectId,
        new_importance_from_queries: Importance,
    ) -> crate::Result<()> {
        if let Some(o) = self.0.lock().await.objects.get_mut(&object_id) {
            o.importance_from_queries = new_importance_from_queries;
        }
        // Always return Ok, even if there's no object it just means it was already unlocked and vacuumed
        Ok(())
    }

    // TODO(test-high): call notify_*_removals and check that memdb and dbs return the same
    async fn client_vacuum(
        &self,
        _notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        _notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // get a real, splittable borrow
        this.objects
            .retain(|_, o| o.importance.lock() || o.importance_from_queries.lock());
        this.binaries.retain(|b, _| {
            this.objects
                .values()
                .any(|o| o.required_binaries.contains(b))
        });
        Ok(())
    }

    async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        unimplemented!() // TODO(test-high)
    }

    async fn get_upload(&self, _upload_id: UploadId) -> crate::Result<Option<Upload>> {
        unimplemented!() // TODO(test-high)
    }

    async fn enqueue_upload(
        &self,
        _upload: Upload,
        _required_binaries: Vec<BinPtr>,
    ) -> crate::Result<UploadId> {
        unimplemented!() // TODO(test-high)
    }

    async fn upload_finished(&self, _upload_id: UploadId) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }

    async fn get_saved_objects(&self) -> crate::Result<HashMap<ObjectId, SavedObjectMeta>> {
        unimplemented!() // TODO(test-high)
    }

    async fn get_saved_queries(&self) -> crate::Result<HashMap<QueryId, SavedQuery>> {
        unimplemented!() // TODO(test-high)
    }

    async fn record_query(
        &self,
        _query_id: QueryId,
        _query: Arc<Query>,
        _type_id: TypeId,
        _importance: Importance,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }

    async fn set_query_importance(
        &self,
        _query_id: QueryId,
        _importance: Importance,
        _objects_matching_query: Vec<ObjectId>,
    ) -> crate::Result<()> {
        unimplemented!()
    }

    async fn forget_query(
        &self,
        _query_id: QueryId,
        _objects_to_unlock: Vec<ObjectId>,
    ) -> crate::Result<()> {
        unimplemented!()
    }

    async fn update_queries(
        &self,
        _queries: &HashSet<QueryId>,
        _now_have_all_until: Updatedness,
    ) -> crate::Result<()> {
        unimplemented!() // TODO(test-high)
    }
}
