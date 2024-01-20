use crate::{
    api::parse_snapshot_js,
    db_trait::Db,
    error::ResultExt,
    full_object::{Change, FullObject},
    BinPtr, CanDoCallbacks, CrdbStream, DbPtr, Event, EventId, Object, ObjectId, Query, Timestamp,
    TypeId, User,
};
use anyhow::anyhow;
use futures::{future, TryFutureExt};
use js_sys::Array;
use std::{
    collections::{BTreeMap, HashSet},
    ops::Bound,
    sync::Arc,
};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct SnapshotMeta {
    snapshot_id: EventId,
    type_id: TypeId,
    object_id: ObjectId,
    is_creation: Option<usize>, // IndexedDB cannot index booleans, but never indexes missing properties
    is_latest: Option<usize>,   // So, use None for "false" and Some(1) for "true"
    snapshot_version: i32,
    is_locked: Option<usize>, // Only set on creation snapshot, Some(1) if locked and Some(0) if not
    upload_not_over: Option<usize>, // Only set on creation snapshot, Some(1) if not uploaded yet and Some(0) if uploaded
    required_binaries: Vec<BinPtr>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct EventMeta {
    event_id: EventId,
    object_id: ObjectId,
    upload_not_over: usize, // 1 if not uploaded already and 0 if not
    required_binaries: Vec<BinPtr>,
}

pub struct IndexedDb {
    is_persistent: bool,
    db: indexed_db::Database<crate::Error>,
}

impl IndexedDb {
    pub async fn connect(url: &str) -> anyhow::Result<IndexedDb> {
        let window = web_sys::window().ok_or_else(|| anyhow!("not running in a browser"))?;
        let is_persistent = JsFuture::from(window.navigator().storage().persist().wrap_context(
            "failed to request persistence, did the user disable storage altogether?",
        )?)
        .await
        .wrap_context("failed to resolve request for persistence")?
        .as_bool()
        .ok_or_else(|| anyhow!("requesting for persistence did not return a boolean"))?;

        let factory = indexed_db::Factory::get().wrap_context("getting IndexedDb factory")?;

        const VERSION: u32 = 1;
        let db = factory
            .open(url, VERSION, |evt| async move {
                let db = evt.database();

                db.build_object_store("snapshots").create()?;
                db.build_object_store("events").create()?;
                db.build_object_store("binaries").create()?;
                let snapshots_meta = db
                    .build_object_store("snapshots_meta")
                    .key_path("snapshot_id")
                    .create()?;
                let events_meta = db
                    .build_object_store("events_meta")
                    .key_path("event_id")
                    .create()?;

                snapshots_meta
                    .build_compound_index("latest_object", &["is_latest", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_compound_index("creation_object", &["is_creation", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_compound_index("locked_object", &["is_locked", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_compound_index("not_uploaded_object", &["upload_not_over", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_compound_index("object_snapshot", &["object_id", "snapshot_id"])
                    .create()?;
                snapshots_meta
                    .build_index("required_binaries", "required_binaries")
                    .multi_entry()
                    .create()?;

                events_meta
                    .build_compound_index("not_uploaded_event", &["upload_not_over", "event_id"])
                    .unique()
                    .create()?;
                events_meta
                    .build_compound_index("object_event", &["object_id", "event_id"])
                    .create()?;
                events_meta
                    .build_index("required_binaries", "required_binaries")
                    .multi_entry()
                    .create()?;

                Ok(())
            })
            .await
            .wrap_with_context(|| format!("opening IndexedDb {url:?} at version {VERSION}"))?;

        Ok(IndexedDb { is_persistent, db })
    }

    pub fn is_persistent(&self) -> bool {
        self.is_persistent
    }

    #[allow(dead_code)] // TODO: use in vacuum
    async fn list_required_binaries(
        transaction: &indexed_db::Transaction<crate::Error>,
    ) -> crate::Result<HashSet<BinPtr>> {
        let snapshots_meta = transaction
            .object_store("snapshots_meta")
            .wrap_context("opening 'snapshots_meta' object store")?;
        let events_meta = transaction
            .object_store("events_meta")
            .wrap_context("opening 'events_meta' object store")?;

        let mut required_binaries = HashSet::new();
        let mut s = snapshots_meta
            .cursor()
            .open()
            .await
            .wrap_context("opening cursor on 'snapshots_meta'")?;
        while let Some(m) = s.value() {
            required_binaries.extend(
                serde_wasm_bindgen::from_value::<SnapshotMeta>(m)
                    .wrap_context("parsing snapshot metadata")?
                    .required_binaries,
            );
            s.advance(1)
                .await
                .wrap_context("moving to next snapshot metadata cursor item")?;
        }
        let mut e = events_meta
            .cursor()
            .open()
            .await
            .wrap_context("opening cursor on 'events_meta'")?;
        while let Some(m) = e.value() {
            required_binaries.extend(
                serde_wasm_bindgen::from_value::<EventMeta>(m)
                    .wrap_context("parsing event metadata")?
                    .required_binaries,
            );
            e.advance(1)
                .await
                .wrap_context("moving to next event metadata cursor item")?;
        }

        Ok(required_binaries)
    }

    #[cfg(feature = "_tests")]
    pub async fn assert_invariants_generic(&self) {
        use std::collections::{hash_map, HashMap};

        self.db
            .transaction(&[
                "snapshots",
                "snapshots_meta",
                "events",
                "events_meta",
                "binaries",
            ])
            .run(move |transaction| async move {
                let snapshots_meta = transaction.object_store("snapshots_meta").unwrap();
                let events_meta = transaction.object_store("events_meta").unwrap();
                let binaries = transaction.object_store("binaries").unwrap();

                let creation_object = snapshots_meta.index("creation_object").unwrap();

                // All binaries are present
                let required_binaries = Self::list_required_binaries(&transaction).await.unwrap();
                for b in required_binaries {
                    if !binaries
                        .contains(&serde_wasm_bindgen::to_value(&b).unwrap())
                        .await
                        .unwrap()
                    {
                        panic!("missing required binary {b:?}");
                    }
                }

                // No event references an object without a creation snapshot
                let mut event_cursor = events_meta.cursor().open().await.unwrap();
                while let Some(e) = event_cursor.value() {
                    let e = serde_wasm_bindgen::from_value::<EventMeta>(e).unwrap();
                    if !creation_object
                        .contains(&Array::from_iter([
                            &JsValue::from(1),
                            &e.object_id.to_js_string(),
                        ]))
                        .await
                        .unwrap()
                    {
                        panic!(
                            "event {:?} references object {:?} that has no creation snapshot",
                            e.event_id, e.object_id
                        );
                    }
                    event_cursor.advance(1).await.unwrap();
                }

                // All non-creation snapshots match an event, on the same object
                let mut snapshot_cursor = snapshots_meta.cursor().open().await.unwrap();
                while let Some(s) = snapshot_cursor.value() {
                    let s = serde_wasm_bindgen::from_value::<SnapshotMeta>(s).unwrap();
                    let e = events_meta
                        .get(&s.snapshot_id.to_js_string())
                        .await
                        .unwrap();
                    if s.is_creation.is_none() && !e.is_some() {
                        panic!("snapshot {:?} has no corresponding event", s.snapshot_id);
                    }
                    if let Some(e) = e {
                        let e = serde_wasm_bindgen::from_value::<EventMeta>(e).unwrap();
                        if e.object_id != s.object_id {
                            panic!(
                                "object for snapshot and event at {:?} does not match",
                                s.snapshot_id
                            );
                        }
                    }
                    snapshot_cursor.advance(1).await.unwrap();
                }

                // All objects have a single type
                let mut snapshot_cursor = snapshots_meta.cursor().open().await.unwrap();
                let mut types = HashMap::new();
                while let Some(s) = snapshot_cursor.value() {
                    let s = serde_wasm_bindgen::from_value::<SnapshotMeta>(s).unwrap();
                    match types.entry(s.object_id) {
                        hash_map::Entry::Occupied(o) => {
                            if *o.get() != s.type_id {
                                panic!("object {:?} has multiple type ids", s.object_id);
                            }
                        }
                        hash_map::Entry::Vacant(v) => {
                            v.insert(s.type_id);
                        }
                    }
                    snapshot_cursor.advance(1).await.unwrap();
                }

                Ok(())
            })
            .await
            .unwrap();
    }

    #[cfg(feature = "_tests")]
    pub async fn assert_invariants_for<T: Object>(&self) {
        use std::collections::HashMap;

        self.db
            .transaction(&[
                "snapshots",
                "snapshots_meta",
                "events",
                "events_meta",
                "binaries",
            ])
            .run(move |transaction| async move {
                let snapshots = transaction.object_store("snapshots").unwrap();
                let snapshots_meta = transaction.object_store("snapshots_meta").unwrap();
                let events = transaction.object_store("events").unwrap();
                let events_meta = transaction.object_store("events_meta").unwrap();

                // Fetch all snapshots
                let snapshots = snapshots_meta.get_all(None).await.unwrap();
                let snapshots = snapshots
                    .into_iter()
                    .map(|s| serde_wasm_bindgen::from_value::<SnapshotMeta>(s).unwrap())
                    .collect::<Vec<_>>();
                let objects = snapshots
                    .iter()
                    .map(|s| s.object_id)
                    .collect::<HashSet<_>>();
                let mut object_snapshots_map = HashMap::new();
                for s in snapshots {
                    object_snapshots_map
                        .entry(s.object_id)
                        .or_insert_with(BTreeMap::new)
                        .insert(s.snapshot_id, s);
                }

                // Fetch all events
                let events = events_meta.get_all(None).await.unwrap();
                let events = events
                    .into_iter()
                    .map(|e| serde_wasm_bindgen::from_value::<EventMeta>(e).unwrap())
                    .collect::<Vec<_>>();
                let mut object_events_map = HashMap::new();
                for o in objects.iter() {
                    object_events_map.insert(*o, BTreeMap::new());
                }
                for e in events {
                    object_events_map
                        .get_mut(&e.object_id)
                        .unwrap()
                        .insert(e.event_id, e);
                }

                // For each object
                for o in objects {
                    let snapshots = object_snapshots_map.get(&o).unwrap();
                    let events = object_events_map.get(&o).unwrap();

                    // It has a creation and a latest snapshot that surround all other snapshots
                    let creation = snapshots.first_key_value().unwrap().1;
                    let latest = snapshots.last_key_value().unwrap().1;
                    assert!(creation.is_creation == Some(1));
                    assert!(latest.is_latest == Some(1));
                    if creation.snapshot_id == latest.snapshot_id {
                        continue;
                    }

                    // Creation and latest surround all other events
                    assert!(events.first_key_value().unwrap().0 > &creation.snapshot_id);
                    assert!(events.last_key_value().unwrap().0 == &latest.snapshot_id);
                }

                Ok(())
            })
            .await
            .unwrap();
    }
}

impl Db for IndexedDb {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        _cb: &C,
    ) -> crate::Result<()> {
        let new_snapshot_meta = SnapshotMeta {
            snapshot_id: created_at,
            type_id: *T::type_ulid(),
            object_id,
            is_creation: Some(1),
            is_latest: Some(1),
            snapshot_version: T::snapshot_version(),
            is_locked: Some(0), // TODO: allow for atomic create-and-lock
            upload_not_over: Some(1),
            required_binaries: object.required_binaries(),
        };
        let object_id_js = object_id.to_js_string();
        let new_snapshot_id_js = created_at.to_js_string();
        let new_snapshot_meta_js = serde_wasm_bindgen::to_value(&new_snapshot_meta)
            .wrap_with_context(|| format!("serializing metadata for {object_id:?}"))?;
        let new_snapshot_js = serde_wasm_bindgen::to_value(&*object)
            .wrap_with_context(|| format!("serializing {object_id:?}"))?;
        let required_binaries = object.required_binaries();

        self.db
            .transaction(&["snapshots", "snapshots_meta", "events", "binaries"])
            .rw()
            .run(move |transaction| async move {
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?;
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving 'snapshots_meta' object store")?;
                let events = transaction
                    .object_store("events")
                    .wrap_context("retrieving 'events' object store")?;
                let binaries = transaction
                    .object_store("binaries")
                    .wrap_context("retrieving 'binaries' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving 'creation_object' index")?;

                // First, check for absence of object id conflict
                if let Some(old_meta_js) = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_context("checking whether {object_id:?} already existed")?
                {
                    // Snapshot metadata for this object already exists. Check that the already-existing value was the same
                    let mut old_meta = serde_wasm_bindgen::from_value::<SnapshotMeta>(old_meta_js)
                        .wrap_with_context(|| {
                            format!("deserializing preexisting snapshot metadata for {object_id:?}")
                        })?;
                    // Ignore a few fields in comparison below
                    old_meta.is_latest = Some(1);
                    old_meta.is_locked = Some(0);
                    old_meta.upload_not_over = Some(1);
                    if old_meta != new_snapshot_meta {
                        return Err(crate::Error::ObjectAlreadyExists(object_id).into());
                    }

                    // Metadata is the same, still need to check snapshot contents
                    let old_data_js = snapshots
                        .get(&new_snapshot_id_js)
                        .await
                        .wrap_with_context(|| {
                            format!("retrieving snapshot data for {created_at:?}")
                        })?
                        .ok_or_else(|| {
                            crate::Error::Other(anyhow!(
                                "Snapshot metadata existed without data for {created_at:?}"
                            ))
                        })?;
                    let old_data = parse_snapshot_js::<T>(old_meta.snapshot_version, old_data_js)
                        .wrap_with_context(|| {
                        format!("deserializing preexisting snapshot for {created_at:?}")
                    })?;
                    if old_data != *object {
                        return Err(crate::Error::ObjectAlreadyExists(object_id).into());
                    }

                    // The old snapshot and data were the same, we're good to go
                    return Ok(());
                }

                // The object didn't exist yet, try inserting it
                match snapshots_meta.add(&new_snapshot_meta_js).await {
                    Err(indexed_db::Error::AlreadyExists) => {
                        // `created_at` already exists, but we already checked that `object_id` did not. This is a collision.
                        Err(crate::Error::EventAlreadyExists(created_at).into())
                    }
                    Err(e) => Err(e),
                    Ok(_) => {
                        // Snapshot metadata addition succeeded. Now, time to add the data itself
                        snapshots
                            .add_kv(&new_snapshot_id_js, &new_snapshot_js)
                            .await
                            .wrap_with_context(|| {
                                format!("saving new snapshot {created_at:?} in database")
                            })?;

                        // Check for no event id conflict
                        if events
                            .contains(&new_snapshot_id_js)
                            .await
                            .wrap_with_context(|| {
                                format!(
                                    "checking whether {created_at:?} already existed as an event"
                                )
                            })?
                        {
                            return Err(crate::Error::EventAlreadyExists(created_at).into());
                        }

                        // Finally, validate the required binaries
                        check_required_binaries(binaries, required_binaries).await?;

                        Ok(())
                    }
                }
            })
            .await
            .wrap_with_context(|| format!("running creation transaction for {object_id:?}"))
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> crate::Result<()> {
        let new_event_meta = EventMeta {
            event_id,
            object_id,
            upload_not_over: 1,
            required_binaries: event.required_binaries(),
        };
        let object_id_js = object_id.to_js_string();
        let new_event_id_js = event_id.to_js_string();
        let zero_event_id_js = EventId::from_u128(0).to_js_string();
        let max_event_id_js = EventId::from_u128(u128::MAX).to_js_string();
        let new_event_meta_js = serde_wasm_bindgen::to_value(&new_event_meta)
            .wrap_with_context(|| format!("serializing {event_id:?}"))?;
        let new_event_js = serde_wasm_bindgen::to_value(&*event)
            .wrap_with_context(|| format!("serializing {event_id:?}"))?;

        self.db
            .transaction(&["snapshots", "snapshots_meta", "events", "events_meta", "binaries"])
            .rw()
            .run(move |transaction| async move {
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?;
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving 'snapshots_meta' object store")?;
                let events = transaction
                    .object_store("events")
                    .wrap_context("retrieving 'events' object store")?;
                let events_meta = transaction
                    .object_store("events_meta")
                    .wrap_context("retrieving 'events_meta' object store")?;
                let binaries = transaction
                    .object_store("binaries")
                    .wrap_context("retrieving 'binaries' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving 'creation_object' index")?;
                let object_snapshot = snapshots_meta
                    .index("object_snapshot")
                    .wrap_context("retrieving 'object_snapshot' index")?;

                // Check the object does exist, is of the right type and is not too new
                let Some(creation_snapshot_js) = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_with_context(|| format!("checking that {object_id:?} already exists"))?
                else {
                    return Err(crate::Error::ObjectDoesNotExist(object_id).into());
                };
                let creation_snapshot =
                    serde_wasm_bindgen::from_value::<SnapshotMeta>(creation_snapshot_js)
                        .wrap_with_context(|| {
                            format!("deserializing creation snapshot metadata for {object_id:?}")
                        })?;
                if creation_snapshot.type_id != *T::type_ulid() {
                    return Err(crate::Error::WrongType {
                        object_id,
                        expected_type_id: *T::type_ulid(),
                        real_type_id: creation_snapshot.type_id,
                    }
                    .into());
                }
                if creation_snapshot.snapshot_id >= event_id {
                    return Err(crate::Error::EventTooEarly {
                        event_id,
                        object_id,
                        created_at: creation_snapshot.snapshot_id,
                    }
                    .into());
                }

                // Insert the event metadata, checking for collisions
                match events_meta.add(&new_event_meta_js).await {
                    Err(indexed_db::Error::AlreadyExists) => {
                        // Got a collision. Check whether the event already exist in the database.
                        let old_meta_js = events_meta.get(&new_event_id_js)
                            .await
                            .wrap_with_context(|| format!("retrieving pre-existing event metadata for {event_id:?}"))?
                            .ok_or_else(|| {
                                crate::Error::Other(anyhow!("inserting {event_id:?} failed but the preexisting duplicate seems not to exist"))
                            })?;
                        let mut old_meta = serde_wasm_bindgen::from_value::<EventMeta>(old_meta_js)
                            .wrap_with_context(|| {
                                format!("deserializing preexisting event metadata for {event_id:?}")
                            })?;
                        // Ignore a few fields in comparison below
                        old_meta.upload_not_over = 1;
                        if old_meta != new_event_meta {
                            return Err(crate::Error::EventAlreadyExists(event_id).into());
                        }

                        // Metadata is the same, still need to check event contents
                        let old_data_js = events
                            .get(&new_event_id_js)
                            .await
                            .wrap_with_context(|| {
                                format!("retrieving event data for {event_id:?}")
                            })?
                            .ok_or_else(|| {
                                crate::Error::Other(anyhow!(
                                    "Event metadata existed without data for {event_id:?}"
                                ))
                            })?;
                        let old_data = serde_wasm_bindgen::from_value::<T::Event>(old_data_js)
                            .wrap_with_context(|| {
                                format!("deserializing preexisting event data for {event_id:?}")
                            })?;
                        if old_data != *event {
                            return Err(crate::Error::EventAlreadyExists(event_id).into());
                        }

                        // The old snapshot and data were the same, we're good to go
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                    Ok(_) => (),
                }

                // Insert the event itself
                events.add_kv(&new_event_id_js, &new_event_js).await.wrap_with_context(|| format!("saving {event_id:?} in database"))?;

                // Clear all snapshots after the event
                let mut to_clear = object_snapshot
                    .cursor()
                    .range(&**Array::from_iter([&object_id_js, &new_event_id_js])..=&**Array::from_iter([&object_id_js, &max_event_id_js]))
                    .wrap_with_context(|| format!("limiting the snapshots to delete to only those of {object_id:?} after {event_id:?}"))?
                    .open()
                    .await
                    .wrap_with_context(|| format!("opening cursor of snapshots to delete for {object_id:?} after {event_id:?}"))?;
                while let Some(snapshot_meta_js) = to_clear.value() {
                    let snapshot_meta = serde_wasm_bindgen::from_value::<SnapshotMeta>(snapshot_meta_js)
                        .wrap_with_context(|| format!("deserializing to-clear snapshot of {object_id:?}, after {event_id:?}"))?;
                    let snapshot_id = snapshot_meta.snapshot_id;
                    snapshots.delete(&snapshot_id.to_js_string()).await.wrap_with_context(|| format!("deleting snapshot {snapshot_id:?}"))?;
                    to_clear.delete().await.wrap_with_context(|| format!("deleting snapshot metadata {snapshot_id:?}"))?;
                    to_clear.advance(1).await.wrap_context("getting next snapshot to delete")?;
                }

                // Find the last snapshot for the object
                let last_snapshot_meta_js = object_snapshot
                    .cursor()
                    .range(&**Array::from_iter([&object_id_js, &zero_event_id_js])..=&**Array::from_iter([&object_id_js, &max_event_id_js]))
                    .wrap_with_context(|| format!("limiting the last snapshot to recover to those of {object_id:?}"))?
                    .direction(indexed_db::CursorDirection::Prev)
                    .open()
                    .await
                    .wrap_with_context(|| format!("opening cursor for the last snapshot of {object_id:?}"))?
                    .value()
                    .ok_or_else(|| crate::Error::Other(anyhow!("cannot find a latest snapshot for {object_id:?}")))?;
                let mut last_snapshot_meta = serde_wasm_bindgen::from_value::<SnapshotMeta>(last_snapshot_meta_js)
                    .wrap_with_context(|| format!("deserializing the last known snapshot for {object_id:?}"))?;
                let last_snapshot_id = last_snapshot_meta.snapshot_id;
                let last_snapshot_id_js = last_snapshot_id.to_js_string();
                let last_snapshot_js = snapshots.get(&last_snapshot_id_js)
                    .await
                    .wrap_with_context(|| format!("retrieving last available snapshot {last_snapshot_id:?}"))?
                    .ok_or_else(|| crate::Error::Other(anyhow!("cannot retrieve snapshot data for {last_snapshot_id:?}")))?;
                let mut last_snapshot = parse_snapshot_js::<T>(last_snapshot_meta.snapshot_version, last_snapshot_js)
                    .wrap_with_context(|| format!("deserializing snapshot data {last_snapshot_id:?}"))?;

                // Mark it as non-latest
                if last_snapshot_meta.is_latest.is_some() {
                    last_snapshot_meta.is_latest = None;
                    let last_snapshot_meta_js = serde_wasm_bindgen::to_value(&last_snapshot_meta)
                        .wrap_with_context(|| format!("reserializing {last_snapshot_id:?}"))?;
                    snapshots_meta.put(&last_snapshot_meta_js)
                        .await
                        .wrap_with_context(|| format!("marking {last_snapshot_id:?} as not the latest any longer"))?;
                }

                // Apply all the events since the last snapshot (excluded)
                let last_applied_event_id = apply_events_after(&transaction, &mut last_snapshot, object_id, last_snapshot_id).await?;

                // Save the new last snapshot
                let new_last_snapshot_meta = SnapshotMeta {
                    snapshot_id: last_applied_event_id,
                    type_id: *T::type_ulid(),
                    object_id,
                    is_creation: None,
                    is_latest: Some(1),
                    snapshot_version: T::snapshot_version(),
                    is_locked: None,
                    upload_not_over: None,
                    required_binaries: last_snapshot.required_binaries(),
                };
                let last_applied_event_id_js = serde_wasm_bindgen::to_value(&last_applied_event_id)
                    .wrap_with_context(|| format!("serializing {last_applied_event_id:?}"))?;
                let new_last_snapshot_meta_js = serde_wasm_bindgen::to_value(&new_last_snapshot_meta)
                    .wrap_with_context(|| format!("serializing the snapshot metadata for {object_id:?} at {last_applied_event_id:?}"))?;
                let new_last_snapshot_js = serde_wasm_bindgen::to_value(&last_snapshot)
                    .wrap_with_context(|| format!("serializing the snapshot for {object_id:?} at {last_applied_event_id:?}"))?;
                snapshots_meta.add(&new_last_snapshot_meta_js)
                    .await
                    .wrap_with_context(|| format!("saving new last snapshot metadata for {object_id:?} at {last_applied_event_id:?}"))?;
                snapshots.add_kv(&last_applied_event_id_js, &new_last_snapshot_js)
                    .await
                    .wrap_with_context(|| format!("saving new last snapshot data for {object_id:?} at {last_applied_event_id:?}"))?;

                // And finally, check that all required binaries are present
                check_required_binaries(binaries, event.required_binaries()).await
                    .wrap_with_context(|| format!("checking that all the binaries required by {event_id:?} are already present"))?;

                Ok(())
            })
            .await
            .wrap_with_context(|| {
                format!("running submission creation for {event_id:?} on {object_id:?}")
            })
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        let object_id_js = object_id.to_js_string();
        self.db
            .transaction(&["snapshots", "snapshots_meta", "events", "events_meta"])
            .run(move |transaction| async move {
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?;
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving 'snapshots_meta' object store")?;
                let events = transaction
                    .object_store("events")
                    .wrap_context("retrieving 'events' object store")?;
                let events_meta = transaction
                    .object_store("events_meta")
                    .wrap_context("retrieving 'events_meta' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving 'creation_object' index")?;
                let latest_object = snapshots_meta
                    .index("latest_object")
                    .wrap_context("retrieving 'latest_object' index")?;
                let object_event = events_meta
                    .index("object_event")
                    .wrap_context("retrieving 'object_event' index")?;

                // Figure out the creation snapshot
                let creation_snapshot_meta_js = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_with_context(|| {
                        format!("fetching creation snapshot metadata for {object_id:?}")
                    })?
                    .ok_or_else(|| crate::Error::ObjectDoesNotExist(object_id))?;
                let creation_snapshot_meta =
                    serde_wasm_bindgen::from_value::<SnapshotMeta>(creation_snapshot_meta_js)
                        .wrap_with_context(|| {
                            format!("deserializing creation snapshot metadata for {object_id:?}")
                        })?;
                if creation_snapshot_meta.type_id != *T::type_ulid() {
                    return Err(crate::Error::WrongType {
                        object_id,
                        expected_type_id: *T::type_ulid(),
                        real_type_id: creation_snapshot_meta.type_id,
                    }
                    .into());
                }
                let creation_snapshot_id = creation_snapshot_meta.snapshot_id;
                let creation_snapshot_id_js = creation_snapshot_id.to_js_string();
                let creation_snapshot_js = snapshots
                    .get(&creation_snapshot_id_js)
                    .await
                    .wrap_with_context(|| {
                        format!("fetching snapshot data for {creation_snapshot_id:?}")
                    })?
                    .ok_or_else(|| {
                        crate::Error::Other(anyhow!(
                            "failed to recover data for snapshot {creation_snapshot_id:?}"
                        ))
                    })?;
                let creation_snapshot = Arc::new(
                    parse_snapshot_js::<T>(
                        creation_snapshot_meta.snapshot_version,
                        creation_snapshot_js,
                    )
                    .wrap_with_context(|| {
                        format!("deserializing snapshot data for {creation_snapshot_id:?}")
                    })?,
                );

                // Figure out the latest snapshot
                let latest_snapshot_meta_js = latest_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_with_context(|| {
                        format!("fetching latest snapshot metadata for {object_id:?}")
                    })?
                    .ok_or_else(|| {
                        crate::Error::Other(anyhow!(
                            "failed to recover metadata for latest snapshot of {object_id:?}"
                        ))
                    })?;
                let latest_snapshot_meta =
                    serde_wasm_bindgen::from_value::<SnapshotMeta>(latest_snapshot_meta_js)
                        .wrap_with_context(|| {
                            format!("deserializing latest snapshot metadata for {object_id:?}")
                        })?;
                let latest_snapshot_id = latest_snapshot_meta.snapshot_id;
                let latest_snapshot_id_js = latest_snapshot_id.to_js_string();
                let latest_snapshot_js = snapshots
                    .get(&latest_snapshot_id_js)
                    .await
                    .wrap_with_context(|| {
                        format!("fetching snapshot data for {latest_snapshot_id:?}")
                    })?
                    .ok_or_else(|| {
                        crate::Error::Other(anyhow!(
                            "failed to recover data for snapshot {latest_snapshot_id:?}"
                        ))
                    })?;
                let latest_snapshot = Arc::new(
                    parse_snapshot_js::<T>(
                        latest_snapshot_meta.snapshot_version,
                        latest_snapshot_js,
                    )
                    .wrap_with_context(|| {
                        format!("deserializing snapshot data for {latest_snapshot_id:?}")
                    })?,
                );

                // List the events in-between
                let mut changes = BTreeMap::new();
                let mut to_apply = object_event
                    .cursor()
                    .range(
                        &**Array::from_iter([&object_id_js, &creation_snapshot_id_js])
                            ..=&**Array::from_iter([&object_id_js, &latest_snapshot_id_js]),
                    )
                    .wrap_with_context(|| format!("filtering only the events for {object_id:?}"))?
                    .open()
                    .await
                    .wrap_with_context(|| format!("listing all events for {object_id:?}"))?;
                while let Some(event_meta_js) = to_apply.value() {
                    let event_meta = serde_wasm_bindgen::from_value::<EventMeta>(event_meta_js)
                        .wrap_context("deserializing event metadata")?;
                    let event_id = event_meta.event_id;
                    let event_id_js = event_id.to_js_string();
                    let event_js = events
                        .get(&event_id_js)
                        .await
                        .wrap_with_context(|| format!("retrieving data for event {event_id:?}"))?
                        .ok_or_else(|| {
                            crate::Error::Other(anyhow!("no data for event {event_id:?}"))
                        })?;
                    let event = serde_wasm_bindgen::from_value::<T::Event>(event_js)
                        .wrap_with_context(|| format!("deserializing event {event_id:?}"))?;
                    changes.insert(event_id, Change::new(Arc::new(event)));
                    to_apply
                        .advance(1)
                        .await
                        .wrap_context("going to next event")?;
                }

                // Add the latest snapshot to the latest event
                if let Some(mut c) = changes.last_entry() {
                    c.get_mut().set_snapshot(latest_snapshot);
                } else {
                    assert!(
                        creation_snapshot_meta.snapshot_id == latest_snapshot_meta.snapshot_id,
                        "got no events but latest_snapshot {:?} != creation_snapshot {:?}",
                        latest_snapshot_meta.snapshot_id,
                        creation_snapshot_meta.snapshot_id,
                    );
                }

                // Finally, return the full object
                Ok(FullObject::from_parts(
                    object_id,
                    creation_snapshot_meta.snapshot_id,
                    creation_snapshot,
                    changes,
                ))
            })
            .await
            .wrap_with_context(|| format!("retrieving {object_id:?} from IndexedDB"))
    }

    async fn query<T: Object>(
        &self,
        _user: User,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        _time: Timestamp,
        _object: ObjectId,
        _cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        todo!()
    }

    async fn create_binary(&self, _binary_id: BinPtr, _data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}

async fn check_required_binaries(
    binaries_store: indexed_db::ObjectStore<crate::Error>,
    binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    let missing_binaries = future::try_join_all(binaries.iter().map(|&b| {
        binaries_store
            .contains(&b.to_js_string())
            .map_ok(move |present| present.then_some(b))
    }))
    .await
    .wrap_with_context(|| format!("checking for required binaries {binaries:?}"))?
    .into_iter()
    .filter_map(|b| b)
    .collect::<Vec<_>>();

    if !missing_binaries.is_empty() {
        return Err(crate::Error::MissingBinaries(missing_binaries));
    }

    Ok(())
}

async fn apply_events_after<T: Object>(
    transaction: &indexed_db::Transaction<crate::Error>,
    object: &mut T,
    object_id: ObjectId,
    mut last_applied_event_id: EventId,
) -> indexed_db::Result<EventId, crate::Error> {
    let events = transaction
        .object_store("events")
        .wrap_context("retrieving 'events' object store")?;
    let object_event = transaction
        .object_store("events_meta")
        .wrap_context("retrieving 'events_meta' object store")?
        .index("object_event")
        .wrap_context("retrieving 'object_event' index")?;

    let object_id_js = object_id.to_js_string();
    let last_applied_event_id_js = last_applied_event_id.to_js_string();
    let max_event_id_js = EventId::from_u128(u128::MAX).to_js_string();

    let mut to_apply = object_event.cursor()
        .range((
            Bound::Excluded(&**Array::from_iter([&object_id_js, &last_applied_event_id_js])),
            Bound::Included(&**Array::from_iter([&object_id_js, &max_event_id_js])),
        ))
        .wrap_with_context(|| format!("limiting the events to apply to those on {object_id:?} since {last_applied_event_id:?}"))?
        .open()
        .await
        .wrap_with_context(|| format!("opening cursor for the events to apply on {object_id:?} since {last_applied_event_id:?}"))?;
    while let Some(apply_event_meta_js) = to_apply.value() {
        let apply_event_meta = serde_wasm_bindgen::from_value::<EventMeta>(apply_event_meta_js)
            .wrap_with_context(|| format!("deserializing event to apply on {object_id:?}"))?;
        let apply_event_id = apply_event_meta.event_id;
        let apply_event_id_js = apply_event_id.to_js_string();
        let apply_event_js = events
            .get(&apply_event_id_js)
            .await
            .wrap_with_context(|| format!("recovering event data for {apply_event_id:?}"))?
            .ok_or_else(|| {
                crate::Error::Other(anyhow!(
                    "no event data for event with metadata {apply_event_id:?}"
                ))
            })?;
        let apply_event = serde_wasm_bindgen::from_value::<T::Event>(apply_event_js)
            .wrap_with_context(|| format!("deserializing event data for {apply_event_id:?}"))?;
        object.apply(DbPtr::from(object_id), &apply_event);
        last_applied_event_id = apply_event_id;
        to_apply
            .advance(1)
            .await
            .wrap_context("getting next event to apply")?;
    }

    Ok(last_applied_event_id)
}
