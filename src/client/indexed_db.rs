use crate::{
    api::{UploadId, UploadOrBinPtr},
    client::ClientStorageInfo,
    db_trait::Db,
    error::ResultExt,
    fts,
    object::parse_snapshot_js,
    BinPtr, CanDoCallbacks, DbPtr, Event, EventId, Object, ObjectId, Query, TypeId,
};
use anyhow::anyhow;
use futures::{future, TryFutureExt};
use indexed_db::CursorDirection;
use js_sys::{Array, JsString, Uint8Array};
use std::{cell::Cell, collections::HashSet, ops::Bound, sync::Arc};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

const OBJECT_STORE_LIST: &[&str] = &[
    "snapshots",
    "events",
    "binaries",
    "upload_queue",
    "snapshots_meta",
    "events_meta",
    "upload_queue_meta",
];

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct SnapshotMeta {
    snapshot_id: EventId,
    type_id: TypeId,
    object_id: ObjectId,
    is_creation: Option<usize>, // IndexedDB cannot index booleans, but never indexes missing properties
    is_latest: Option<usize>,   // So, use None for "false" and Some(1) for "true"
    normalizer_version: i32,
    snapshot_version: i32,
    is_locked: Option<usize>, // Only set on creation snapshot, Some(1) if locked and Some(0) if not
    required_binaries: Vec<BinPtr>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct EventMeta {
    event_id: EventId,
    object_id: ObjectId,
    required_binaries: Vec<BinPtr>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct UploadMeta {
    required_binaries: Vec<BinPtr>,
}

pub struct IndexedDb {
    is_persistent: bool,
    db: indexed_db::Database<crate::Error>,
    objects_unlocked_this_run: Cell<usize>,
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

                // Note: whenever changing this list, remember to also update OBJECT_STORE_LIST
                db.build_object_store("snapshots").create()?;
                db.build_object_store("events").create()?;
                db.build_object_store("binaries").create()?;
                db.build_object_store("upload_queue").create()?;
                let snapshots_meta = db
                    .build_object_store("snapshots_meta")
                    .key_path("snapshot_id")
                    .create()?;
                let events_meta = db
                    .build_object_store("events_meta")
                    .key_path("event_id")
                    .create()?;
                let upload_queue_meta = db
                    .build_object_store("upload_queue_meta")
                    .auto_increment()
                    .create()?;

                snapshots_meta
                    .build_compound_index(
                        "latest_type_object",
                        &["is_latest", "type_id", "object_id"],
                    )
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
                    .build_compound_index("object_snapshot", &["object_id", "snapshot_id"])
                    .create()?;
                snapshots_meta
                    .build_index("required_binaries", "required_binaries")
                    .multi_entry()
                    .create()?;

                events_meta
                    .build_compound_index("object_event", &["object_id", "event_id"])
                    .create()?;
                events_meta
                    .build_index("required_binaries", "required_binaries")
                    .multi_entry()
                    .create()?;

                upload_queue_meta
                    .build_index("required_binaries", "required_binaries")
                    .multi_entry()
                    .create()?;

                Ok(())
            })
            .await
            .wrap_with_context(|| format!("opening IndexedDb {url:?} at version {VERSION}"))?;

        Ok(IndexedDb {
            is_persistent,
            db,
            objects_unlocked_this_run: Cell::new(0),
        })
    }

    #[cfg(feature = "_tests")]
    pub fn close(&self) {
        self.db.close();
    }

    pub fn is_persistent(&self) -> bool {
        self.is_persistent
    }

    async fn list_required_binaries(
        transaction: &indexed_db::Transaction<crate::Error>,
    ) -> crate::Result<HashSet<BinPtr>> {
        let snapshots_meta = transaction
            .object_store("snapshots_meta")
            .wrap_context("opening 'snapshots_meta' object store")?;
        let events_meta = transaction
            .object_store("events_meta")
            .wrap_context("opening 'events_meta' object store")?;
        let upload_queue_meta = transaction
            .object_store("upload_queue_meta")
            .wrap_context("opening 'upload_queue_meta' object store")?;

        let snapshot_required_binaries = snapshots_meta
            .index("required_binaries")
            .wrap_context("opening 'required_binaries' snapshot index")?;
        let event_required_binaries = events_meta
            .index("required_binaries")
            .wrap_context("opening 'required_binaries' event index")?;
        let upload_queue_required_binaries = upload_queue_meta
            .index("required_binaries")
            .wrap_context("opening 'required_binaries' upload_queue index")?;

        let required_binaries = snapshot_required_binaries
            .get_all_keys(None)
            .await
            .wrap_context("listing all required binaries for snapshots")?
            .into_iter()
            .chain(
                event_required_binaries
                    .get_all_keys(None)
                    .await
                    .wrap_context("listing all required binaries for events")?
                    .into_iter(),
            )
            .chain(
                upload_queue_required_binaries
                    .get_all_keys(None)
                    .await
                    .wrap_context("listing all required binaries for the upload queue")?
                    .into_iter(),
            );
        let mut res = HashSet::new();
        for b in required_binaries {
            let b = serde_wasm_bindgen::from_value::<BinPtr>(b).wrap_context("parsing BinPtr")?;
            res.insert(b);
        }

        Ok(res)
    }

    pub async fn storage_info(&self) -> crate::Result<ClientStorageInfo> {
        let window = web_sys::window()
            .ok_or_else(|| crate::Error::Other(anyhow!("not running in a browser")))?;
        let storage = window.navigator().storage();
        let estimate = storage.estimate().map_err(|_| {
            crate::Error::Other(anyhow!(
                "failed getting a storage estimate from the browser"
            ))
        })?;
        let estimate = wasm_bindgen_futures::JsFuture::from(estimate)
            .await
            .map_err(|_| {
                crate::Error::Other(anyhow!("storage estimate promise returned an error"))
            })?;
        let quota = js_sys::Reflect::get(&estimate, &JsString::from("quota"))
            .map_err(|_| crate::Error::Other(anyhow!("storage estimate had no quota field")))?
            .as_f64()
            .ok_or_else(|| {
                crate::Error::Other(anyhow!("storage estimate for quota was not a number"))
            })? as usize;
        let usage = js_sys::Reflect::get(&estimate, &JsString::from("usage"))
            .map_err(|_| crate::Error::Other(anyhow!("storage estimate had no quota field")))?
            .as_f64()
            .ok_or_else(|| {
                crate::Error::Other(anyhow!("storage estimate for quota was not a number"))
            })? as usize;
        Ok(ClientStorageInfo {
            quota,
            usage,
            objects_unlocked_this_run: self.objects_unlocked_this_run.get(),
        })
    }

    pub async fn query<T: Object>(&self, q: &Query) -> crate::Result<Vec<ObjectId>> {
        q.check()?;
        let type_id_js = T::type_ulid().to_js_string();
        let zero_id = EventId::from_u128(0).to_js_string();
        let max_id = EventId::from_u128(u128::MAX).to_js_string();
        // TODO(low): look into setting up indexes and allowing the user to use them?
        // TODO(low): think a lot about splitting this transaction to be able to return a real stream by
        // using cursors? The difficulty will be that another task could clobber the latest index
        // during that time.

        // List all objects matching the query
        let objects = self
            .db
            .transaction(&["snapshots_meta", "snapshots"])
            .run(move |transaction| async move {
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?;
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving 'snapshots_meta' object store")?;

                let latest_type_object = snapshots_meta
                    .index("latest_type_object")
                    .wrap_context("retrieving 'latest_type_object' index")?;

                let mut cursor = latest_type_object
                    .cursor()
                    .range(
                        &**Array::from_iter([&JsValue::from(1), &type_id_js, &zero_id])
                            ..=&**Array::from_iter([&JsValue::from(1), &type_id_js, &max_id]),
                    )
                    .wrap_context("limiting cursor to only snapshots of the right type")?
                    .open_key()
                    .await
                    .wrap_context("opening cursor over all latest objects")?;
                let mut objects = Vec::new();
                while let Some(snapshot_id_js) = cursor.primary_key() {
                    let snapshot = snapshots
                        .get(&snapshot_id_js)
                        .await
                        .wrap_context("retrieving snapshot data for known metadata")?
                        .ok_or_else(|| {
                            crate::Error::Other(anyhow!("no snapshot data for known metadata"))
                        })?;
                    let snapshot = serde_wasm_bindgen::from_value::<serde_json::Value>(snapshot)
                        .wrap_context("deserializing snapshot data as serde_json::Value")?;
                    if q.matches_json(&snapshot) {
                        let object_id_js = cursor
                            .key()
                            .ok_or_else(|| {
                                crate::Error::Other(anyhow!("cursor had a primary key but no key"))
                            })?
                            .dyn_into::<Array>()
                            .wrap_context("cursor key was not an array")?
                            .get(2);
                        let object_id = serde_wasm_bindgen::from_value::<ObjectId>(object_id_js)
                            .wrap_context("deserializing object id")?;
                        objects.push(object_id);
                    }
                    cursor
                        .advance(1)
                        .await
                        .wrap_context("going to next snapshot in the database")?;
                }
                Ok(objects)
            })
            .await
            .wrap_context("finding a first snapshot to answer")?;

        // Retrieve them one by one
        Ok(objects)
    }

    pub async fn unlock(&self, object_id: ObjectId) -> crate::Result<()> {
        let object_id_js = object_id.to_js_string();

        let res = self
            .db
            .transaction(&["snapshots_meta"])
            .rw()
            .run(move |transaction| async move {
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving the 'snapshots_meta' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving the 'creation_object' index")?;

                let Some(snapshot_js) = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_context("retrieving creation snapshot")?
                else {
                    // Object was already removed from database, so it was already unlocked
                    return Ok(());
                };

                let mut snapshot_meta = serde_wasm_bindgen::from_value::<SnapshotMeta>(snapshot_js)
                    .wrap_context("deserializing snapshot metadata")?;
                snapshot_meta.is_locked = Some(0);
                let snapshot_js = serde_wasm_bindgen::to_value(&snapshot_meta)
                    .wrap_context("reserializing snapshot metadata")?;

                snapshots_meta
                    .put(&snapshot_js)
                    .await
                    .wrap_context("saving the unlocked creation snapshot metadata")?;

                Ok(())
            })
            .await
            .wrap_with_context(|| format!("unlocking {object_id:?} from IndexedDB"));
        if res.is_ok() {
            self.objects_unlocked_this_run
                .set(self.objects_unlocked_this_run.get() + 1);
        }
        res
    }

    pub async fn vacuum(&self) -> crate::Result<()> {
        let zero_id = ObjectId::from_u128(0).to_js_string();
        let max_id = ObjectId::from_u128(u128::MAX).to_js_string();

        let res = self
            .db
            .transaction(&[
                "snapshots",
                "snapshots_meta",
                "events",
                "events_meta",
                "upload_queue_meta",
                "binaries",
            ])
            .rw()
            .run(move |transaction| async move {
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving the 'snapshots_meta' object store")?;
                let events_meta = transaction
                    .object_store("events_meta")
                    .wrap_context("retrieving the 'events_meta' object store")?;
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving the 'snapshots' object store")?;
                let events = transaction
                    .object_store("events")
                    .wrap_context("retrieving the 'events' object store")?;
                let binaries = transaction
                    .object_store("binaries")
                    .wrap_context("retrieving the 'binaries' object store")?;

                let locked_object = snapshots_meta
                    .index("locked_object")
                    .wrap_context("retrieving the 'locked_object' index")?;
                let object_snapshot = snapshots_meta
                    .index("object_snapshot")
                    .wrap_context("retrieving the 'object_snapshot' index")?;

                let object_event = events_meta
                    .index("object_event")
                    .wrap_context("retrieving the 'object_event' index")?;

                // Remove all unlocked objects
                let mut to_remove = locked_object
                    .cursor()
                    .range(
                        &**Array::from_iter([&JsValue::from(0), &zero_id])
                            ..=&**Array::from_iter([&JsValue::from(0), &max_id]),
                    )
                    .wrap_context("limiting cursor to only unlocked objects")?
                    .open()
                    .await
                    .wrap_context("listing unlocked objects")?;
                // TODO(low): could trigger all deletion requests in parallel and only wait for them
                // all before listing still-required binaries, for performance
                while let Some(s) = to_remove.value() {
                    let s = serde_wasm_bindgen::from_value::<SnapshotMeta>(s)
                        .wrap_context("deserializing unlocked object")?;
                    let object_id = s.object_id;
                    let object_id_js = object_id.to_js_string();

                    // Remove all the snapshots
                    let mut snapshots_to_remove = object_snapshot
                        .cursor()
                        .range(
                            &**Array::from_iter([&object_id_js, &zero_id])
                                ..=&**Array::from_iter([&object_id_js, &max_id]),
                        )
                        .wrap_context("limiting the range to to-delete snapshots")?
                        .open()
                        .await
                        .wrap_context("opening cursor of to-delete snapshots")?;
                    while let Some(s) = snapshots_to_remove.value() {
                        let s = serde_wasm_bindgen::from_value::<SnapshotMeta>(s)
                            .wrap_context("deserializing to-remove snapshot metadata")?;
                        snapshots
                            .delete(&s.snapshot_id.to_js_string())
                            .await
                            .wrap_context("failed deleting snapshot")?;
                        snapshots_to_remove.delete().await.wrap_with_context(|| {
                            format!("failed deleting snapshot metadata of {object_id:?}")
                        })?;
                        snapshots_to_remove
                            .advance(1)
                            .await
                            .wrap_context("going to next to-remove snapshot")?;
                    }

                    // Remove all the events
                    let mut events_to_remove = object_event
                        .cursor()
                        .range(
                            &**Array::from_iter([&object_id_js, &zero_id])
                                ..=&**Array::from_iter([&object_id_js, &max_id]),
                        )
                        .wrap_context("limiting the range to to-delete events")?
                        .open()
                        .await
                        .wrap_context("opening cursor of to-delete events")?;
                    while let Some(e) = events_to_remove.value() {
                        let e = serde_wasm_bindgen::from_value::<EventMeta>(e)
                            .wrap_context("deserializing to-remove event metadata")?;
                        events
                            .delete(&e.event_id.to_js_string())
                            .await
                            .wrap_context("failed deleting event")?;
                        events_to_remove.delete().await.wrap_with_context(|| {
                            format!("failed deleting event of {object_id:?}")
                        })?;
                        events_to_remove
                            .advance(1)
                            .await
                            .wrap_context("going to next to-remove event")?;
                    }

                    // Continue
                    to_remove
                        .advance(1)
                        .await
                        .wrap_context("going to next to-remove object")?;
                }

                let required_binaries = Self::list_required_binaries(&transaction)
                    .await
                    .wrap_context("listing still-required binaries")?;
                let mut binaries_cursor = binaries
                    .cursor()
                    .open()
                    .await
                    .wrap_context("opening cursor over all binaries")?;
                while let Some(b) = binaries_cursor.key() {
                    let b = serde_wasm_bindgen::from_value::<BinPtr>(b)
                        .wrap_context("deserializing binary id")?;
                    if !required_binaries.contains(&b) {
                        binaries_cursor
                            .delete()
                            .await
                            .wrap_with_context(|| format!("deleting {b:?}"))?;
                    }
                    binaries_cursor
                        .advance(1)
                        .await
                        .wrap_context("going to next binary")?;
                }

                Ok(())
            })
            .await
            .wrap_context("vacuuming the database");
        if res.is_ok() {
            self.objects_unlocked_this_run.set(0);
        }
        res
    }

    pub async fn list_uploads(&self) -> crate::Result<Vec<UploadId>> {
        // TODO(test): fuzz upload-queue behavior
        self.db
            .transaction(&["upload_queue_meta"])
            .run(move |transaction| async move {
                let keys = transaction
                    .object_store("upload_queue_meta")
                    .wrap_context("retrieving 'upload_queue_meta' object store")?
                    .get_all_keys(None)
                    .await
                    .wrap_context("getting all keys from upload_queue_meta")?;
                let mut res = Vec::with_capacity(keys.len());
                for k in keys.into_iter() {
                    let k = serde_wasm_bindgen::from_value::<UploadId>(k)
                        .wrap_context("deserializing upload id")?;
                    res.push(k);
                }
                Ok(res)
            })
            .await
            .wrap_context("listing upload queue")
    }

    pub async fn get_upload(&self, upload_id: UploadId) -> crate::Result<UploadOrBinPtr> {
        self.db
            .transaction(&["upload_queue"])
            .run(move |transaction| async move {
                let res = transaction
                    .object_store("upload_queue")
                    .wrap_context("retrieving 'upload_queue' object store")?
                    .get(&JsValue::from(upload_id.0))
                    .await
                    .wrap_context("fetching data from upload_queue store")?
                    .ok_or_else(|| indexed_db::Error::DoesNotExist)?;
                let res = serde_wasm_bindgen::from_value::<UploadOrBinPtr>(res)
                    .wrap_context("deserializing data")?;
                Ok(res)
            })
            .await
            .wrap_with_context(|| format!("retrieving data for {upload_id:?}"))
    }

    pub async fn enqueue_upload(
        &self,
        upload: UploadOrBinPtr,
        required_binaries: Vec<BinPtr>,
    ) -> crate::Result<UploadId> {
        let metadata = UploadMeta { required_binaries };
        let metadata =
            serde_wasm_bindgen::to_value(&metadata).wrap_context("serializing upload metadata")?;
        let data = serde_wasm_bindgen::to_value(&upload).wrap_context("serializing upload data")?;
        self.db
            .transaction(&["upload_queue", "upload_queue_meta"])
            .rw()
            .run(move |transaction| async move {
                let upload_id = transaction
                    .object_store("upload_queue_meta")
                    .wrap_context("retrieving 'upload_queue_meta' object store")?
                    .add(&metadata)
                    .await
                    .wrap_context("saving upload metadata")?;
                transaction
                    .object_store("upload_queue")
                    .wrap_context("retrieving 'upload_queue' object store")?
                    .add_kv(&upload_id, &data)
                    .await
                    .wrap_context("saving upload data")?;
                let upload_id = serde_wasm_bindgen::from_value::<UploadId>(upload_id)
                    .wrap_context("deserializing upload id")?;
                Ok(upload_id)
            })
            .await
            .wrap_context("registering not-yet-completed upload")
    }

    pub async fn upload_finished(&self, upload_id: UploadId) -> crate::Result<()> {
        let res = self
            .db
            .transaction(&["upload_queue", "upload_queue_meta"])
            .rw()
            .run(move |transaction| async move {
                let upload_id = serde_wasm_bindgen::to_value(&upload_id)
                    .wrap_context("serializing upload id")?;
                transaction
                    .object_store("upload_queue")
                    .wrap_context("retrieving 'upload_queue' object store")?
                    .delete(&upload_id)
                    .await
                    .wrap_context("deleting upload data")?;
                transaction
                    .object_store("upload_queue_meta")
                    .wrap_context("retrieving 'upload_queue_meta' object store")?
                    .delete(&upload_id)
                    .await
                    .wrap_context("deleting upload metadata")?;
                Ok(())
            })
            .await
            .wrap_with_context(|| format!("registering {upload_id:?} as having completed"));
        if res.is_ok() {
            self.objects_unlocked_this_run
                .set(self.objects_unlocked_this_run.get() + 1);
        }
        res
    }

    #[cfg(feature = "_tests")]
    pub async fn assert_invariants_generic(&self) {
        use std::collections::{hash_map, HashMap};

        self.db
            .transaction(&[
                "snapshots_meta",
                "events_meta",
                "upload_queue_meta",
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
        use std::collections::{BTreeMap, HashMap};

        self.db
            .transaction(&["snapshots", "snapshots_meta", "events", "events_meta"])
            .run(move |transaction| async move {
                let snapshots_store = transaction.object_store("snapshots").unwrap();
                let snapshots_meta = transaction.object_store("snapshots_meta").unwrap();
                let events_store = transaction.object_store("events").unwrap();
                let events_meta = transaction.object_store("events_meta").unwrap();

                // Fetch all snapshots
                let snapshots = snapshots_meta.get_all(None).await.unwrap();
                let snapshots = snapshots
                    .into_iter()
                    .map(|s| serde_wasm_bindgen::from_value::<SnapshotMeta>(s).unwrap())
                    .collect::<Vec<_>>();
                let objects = snapshots
                    .iter()
                    .filter_map(|s| (s.type_id == *T::type_ulid()).then(|| s.object_id))
                    .collect::<HashSet<_>>();
                let mut object_snapshots_map = HashMap::new();
                for o in objects.iter() {
                    object_snapshots_map.insert(*o, BTreeMap::new());
                }
                for s in snapshots {
                    if let Some(o) = object_snapshots_map.get_mut(&s.object_id) {
                        o.insert(s.snapshot_id, s);
                    }
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
                    if let Some(o) = object_events_map.get_mut(&e.object_id) {
                        o.insert(e.event_id, e);
                    }
                }

                // For each object
                for object_id in objects {
                    let snapshots = object_snapshots_map.get(&object_id).unwrap();
                    let events = object_events_map.get(&object_id).unwrap();

                    // It has a creation and a latest snapshot that surround all other snapshots
                    let creation = snapshots.first_key_value().unwrap().1;
                    let latest = snapshots.last_key_value().unwrap().1;
                    assert!(creation.is_creation == Some(1));
                    assert!(latest.is_latest == Some(1));
                    assert!(creation.is_locked.is_some());
                    if creation.snapshot_id == latest.snapshot_id {
                        continue;
                    }

                    // Creation and latest snapshots surround all other events
                    assert!(events.first_key_value().unwrap().0 > &creation.snapshot_id);
                    assert!(events.last_key_value().unwrap().0 == &latest.snapshot_id);

                    // Rebuilding the object gives the same snapshots
                    let object = snapshots_store
                        .get(&creation.snapshot_id.to_js_string())
                        .await
                        .unwrap()
                        .unwrap();
                    let mut object =
                        parse_snapshot_js::<T>(creation.snapshot_version, object).unwrap();
                    for (event_id, event_meta) in events.iter() {
                        let e = events_store
                            .get(&event_id.to_js_string())
                            .await
                            .unwrap()
                            .unwrap();
                        let e = serde_wasm_bindgen::from_value::<T::Event>(e).unwrap();
                        assert_eq!(event_meta.required_binaries, e.required_binaries());
                        object.apply(DbPtr::from(object_id), &e);
                        if let Some(snapshot_meta) = snapshots.get(&event_id) {
                            let s = snapshots_store
                                .get(&event_id.to_js_string())
                                .await
                                .unwrap()
                                .unwrap();
                            let s =
                                parse_snapshot_js::<T>(snapshot_meta.snapshot_version, s).unwrap();
                            assert!(object == s);
                            assert!(snapshot_meta.required_binaries == object.required_binaries());
                            assert!(snapshot_meta.type_id == *T::type_ulid());
                            assert!(snapshot_meta.is_locked.is_none());
                        }
                    }
                }

                Ok(())
            })
            .await
            .unwrap();
    }

    async fn create_impl<T: Object>(
        &self,
        transaction: indexed_db::Transaction<crate::Error>,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
    ) -> Result<Option<Arc<T>>, indexed_db::Error<crate::Error>> {
        let new_snapshot_meta = SnapshotMeta {
            snapshot_id: created_at,
            type_id: *T::type_ulid(),
            object_id,
            is_creation: Some(1),
            is_latest: Some(1),
            normalizer_version: fts::normalizer_version(),
            snapshot_version: T::snapshot_version(),
            is_locked: Some(lock as usize),
            required_binaries: object.required_binaries(),
        };
        let object_id_js = object_id.to_js_string();
        let new_snapshot_id_js = created_at.to_js_string();
        let new_snapshot_meta_js = serde_wasm_bindgen::to_value(&new_snapshot_meta)
            .wrap_with_context(|| format!("serializing metadata for {object_id:?}"))?;
        let new_snapshot_js = serde_wasm_bindgen::to_value(&*object)
            .wrap_with_context(|| format!("serializing {object_id:?}"))?;
        let required_binaries = object.required_binaries();
        // TODO(low): should make this happen as part of the walk happening anyway in serde_wasm_bindgen::to_value
        crate::check_strings(&serde_json::to_value(&*object).wrap_context("serializing to json")?)?;

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
            old_meta.is_locked = Some(lock as usize);
            if old_meta != new_snapshot_meta {
                return Err(crate::Error::ObjectAlreadyExists(object_id).into());
            }

            // Metadata is the same, still need to check snapshot contents
            let old_data_js = snapshots
                .get(&new_snapshot_id_js)
                .await
                .wrap_with_context(|| format!("retrieving snapshot data for {created_at:?}"))?
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
            return Ok(None);
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
                        format!("checking whether {created_at:?} already existed as an event")
                    })?
                {
                    return Err(crate::Error::EventAlreadyExists(created_at).into());
                }

                // Finally, validate the required binaries
                check_required_binaries(binaries, required_binaries).await?;

                Ok(Some(object))
            }
        }
    }

    pub async fn remove_everything(&self) -> crate::Result<()> {
        self.db
            .transaction(OBJECT_STORE_LIST)
            .rw()
            .run(move |transaction| async move {
                for store in OBJECT_STORE_LIST {
                    transaction
                        .object_store(store)
                        .wrap_with_context(|| format!("retrieving {store:?} object store"))?
                        .clear()
                        .await
                        .wrap_with_context(|| format!("clearing {store:?} object store"))?;
                }
                Ok(())
            })
            .await
            .wrap_context("clearing the IndexedDB database")
    }
}

impl Db for IndexedDb {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        lock: bool,
        _cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        let res = self
            .db
            .transaction(&["snapshots", "snapshots_meta", "events", "binaries"])
            .rw()
            .run(move |transaction| {
                self.create_impl(transaction, object_id, created_at, object, lock)
            })
            .await
            .wrap_with_context(|| format!("running creation transaction for {object_id:?}"));
        if res.is_ok() && !lock {
            self.objects_unlocked_this_run
                .set(self.objects_unlocked_this_run.get() + 1);
        }
        res
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        let new_event_meta = EventMeta {
            event_id,
            object_id,
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

                // TODO(low): should make this happen as part of the walk happening anyway in serde_wasm_bindgen::to_value
                crate::check_strings(&serde_json::to_value(&*event).wrap_context("serializing to json")?)?;

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
                        let old_meta = serde_wasm_bindgen::from_value::<EventMeta>(old_meta_js)
                            .wrap_with_context(|| {
                                format!("deserializing preexisting event metadata for {event_id:?}")
                            })?;
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
                        return Ok(None);
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
                    .direction(CursorDirection::Prev)
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
                    normalizer_version: fts::normalizer_version(),
                    snapshot_version: T::snapshot_version(),
                    is_locked: None,
                    required_binaries: last_snapshot.required_binaries(),
                };
                let last_applied_event_id_js = serde_wasm_bindgen::to_value(&last_applied_event_id)
                    .wrap_with_context(|| format!("serializing {last_applied_event_id:?}"))?;
                let new_last_snapshot_meta_js = serde_wasm_bindgen::to_value(&new_last_snapshot_meta)
                    .wrap_with_context(|| format!("serializing the snapshot metadata for {object_id:?} at {last_applied_event_id:?}"))?;
                let new_last_snapshot_js = serde_wasm_bindgen::to_value(&last_snapshot)
                    .wrap_with_context(|| format!("serializing the snapshot for {object_id:?} at {last_applied_event_id:?}"))?;
                snapshots_meta.add(&new_last_snapshot_meta_js).await.map_err(|err| match err {
                    indexed_db::Error::AlreadyExists => crate::Error::EventAlreadyExists(event_id),
                    e => crate::Error::Other(anyhow::Error::from(e).context(format!("saving new last snapshot metadata for {object_id:?} at {last_applied_event_id:?}"))),

                })?;
                snapshots.add_kv(&last_applied_event_id_js, &new_last_snapshot_js)
                    .await
                    .wrap_with_context(|| format!("saving new last snapshot data for {object_id:?} at {last_applied_event_id:?}"))?;

                // And finally, check that all required binaries are present
                check_required_binaries(binaries, event.required_binaries()).await
                    .wrap_with_context(|| format!("checking that all the binaries required by {event_id:?} are already present"))?;

                Ok(Some(Arc::new(last_snapshot)))
            })
            .await
            .wrap_with_context(|| {
                format!("running submission creation for {event_id:?} on {object_id:?}")
            })
    }

    async fn get_latest<T: Object>(
        &self,
        lock: bool,
        object_id: ObjectId,
    ) -> crate::Result<Arc<T>> {
        let object_id_js = object_id.to_js_string();
        let type_id_js = T::type_ulid().to_js_string();
        let mut transaction =
            self.db
                .transaction(&["snapshots", "snapshots_meta", "events", "events_meta"]);
        if lock {
            transaction = transaction.rw();
        }
        transaction
            .run(move |transaction| async move {
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?;
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving 'snapshots_meta' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving 'creation_object' index")?;
                let latest_type_object = snapshots_meta
                    .index("latest_type_object")
                    .wrap_context("retrieving 'latest_type_object' index")?;

                // Figure out the creation snapshot to validate input
                // TODO(low): this could do with a simple latest_object index
                let creation_snapshot_meta_js = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_with_context(|| {
                        format!("fetching creation snapshot metadata for {object_id:?}")
                    })?
                    .ok_or_else(|| crate::Error::ObjectDoesNotExist(object_id))?;
                let mut creation_snapshot_meta =
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

                // Rewrite the creation snapshot if needed
                if lock && creation_snapshot_meta.is_locked != Some(1) {
                    creation_snapshot_meta.is_locked = Some(1);
                    let new_snapshot_js = serde_wasm_bindgen::to_value(&creation_snapshot_meta)
                        .wrap_context("serializing snapshot metadata")?;
                    snapshots_meta
                        .put(&new_snapshot_js)
                        .await
                        .wrap_with_context(|| {
                            format!("locking creation snapshot for {object_id:?} in database")
                        })?;
                }

                // Get the latest snapshot
                let latest_snapshot_meta_js = latest_type_object
                    .get(&Array::from_iter([
                        &JsValue::from(1),
                        &type_id_js,
                        &object_id_js,
                    ]))
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

                // Return the latest snapshot
                Ok(latest_snapshot)
            })
            .await
            .wrap_with_context(|| format!("retrieving {object_id:?} from IndexedDB"))
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        new_created_at: EventId,
        mut object: Arc<T>,
        force_lock: bool,
        _cb: &C,
    ) -> crate::Result<Option<Arc<T>>> {
        let object_id_js = object_id.to_js_string();
        let type_id_js = T::type_ulid().to_js_string();
        let new_created_at_js = new_created_at.to_js_string();
        let max_id = EventId::from_u128(u128::MAX).to_js_string();
        let zero_id = EventId::from_u128(0).to_js_string();
        let required_binaries = object.required_binaries();

        self.db
            .transaction(&[
                "snapshots",
                "snapshots_meta",
                "events",
                "events_meta",
                "binaries",
            ])
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
                    .wrap_context("retrieving the 'binaries' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving 'creation_object' index")?;
                let latest_type_object = snapshots_meta
                    .index("latest_type_object")
                    .wrap_context("retrieving 'latest_type_object' index")?;
                let object_snapshot = snapshots_meta
                    .index("object_snapshot")
                    .wrap_context("retrieving 'object_snapshot' index")?;

                let object_event = events_meta
                    .index("object_event")
                    .wrap_context("retrieving 'object_event' index")?;

                // Get the current creation snapshot
                let Some(creation_meta) = creation_object
                    .get(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_context("checking creation object")?
                else {
                    // Object does not exist, create it
                    return self
                        .create_impl::<T>(
                            transaction,
                            object_id,
                            new_created_at,
                            object,
                            force_lock,
                        )
                        .await;
                };
                let creation_meta = serde_wasm_bindgen::from_value::<SnapshotMeta>(creation_meta)
                    .wrap_context("parsing snapshot metadata")?;
                if creation_meta.type_id != *T::type_ulid() {
                    return Err(crate::Error::WrongType {
                        object_id,
                        expected_type_id: *T::type_ulid(),
                        real_type_id: creation_meta.type_id,
                    }
                    .into());
                }
                if creation_meta.snapshot_id > new_created_at {
                    return Err(crate::Error::EventTooEarly {
                        event_id: new_created_at,
                        object_id,
                        created_at: creation_meta.snapshot_id,
                    }
                    .into());
                }

                // TODO(low): should make this happen as part of the walk happening anyway in serde_wasm_bindgen::to_value
                crate::check_strings(
                    &serde_json::to_value(&*object).wrap_context("serializing to json")?,
                )?;

                // Check if the requested new_created_at is after the current latest snapshot
                let latest_snapshot_meta_js = latest_type_object
                    .get(&Array::from_iter([
                        &JsValue::from(1),
                        &type_id_js,
                        &object_id_js,
                    ]))
                    .await
                    .wrap_context("retrieving latest snapshot")?
                    .ok_or_else(|| {
                        crate::Error::Other(anyhow!(
                            "No latest snapshot for an object with a creation snapshot"
                        ))
                    })?;
                let latest_snapshot_meta =
                    serde_wasm_bindgen::from_value::<SnapshotMeta>(latest_snapshot_meta_js)
                        .wrap_context("deserializing latest snapshot metadata")?;
                let new_creation_should_be_latest =
                    latest_snapshot_meta.snapshot_id <= new_created_at;

                // Delete all events prior to the new creation snapshot
                let mut to_delete = object_event
                    .cursor()
                    .range(
                        &**Array::from_iter([&object_id_js, &zero_id])
                            ..=&**Array::from_iter([&object_id_js, &new_created_at_js]),
                    )
                    .wrap_context("limiting the range for the events to delete")?
                    .open()
                    .await
                    .wrap_context("opening cursor of all events to delete")?;
                while let Some(event_id_js) = to_delete.primary_key() {
                    events
                        .delete(&event_id_js)
                        .await
                        .wrap_context("deleting event data")?;
                    to_delete
                        .delete()
                        .await
                        .wrap_context("deleting event metadata")?;
                    to_delete
                        .advance(1)
                        .await
                        .wrap_context("moving to next to-apply event")?;
                }

                // Delete all snapshots for the object, we'll recreate creation and latest soon
                let mut to_delete = object_snapshot
                    .cursor()
                    .range(
                        &**Array::from_iter([&object_id_js, &zero_id])
                            ..=&**Array::from_iter([&object_id_js, &max_id]),
                    )
                    .wrap_context("limiting to-delete snapshot range")?
                    .open()
                    .await
                    .wrap_context("opening cursor of snapshots to delete")?;
                while let Some(snapshot_id_js) = to_delete.primary_key() {
                    snapshots
                        .delete(&snapshot_id_js)
                        .await
                        .wrap_context("deleting snapshot data")?;
                    to_delete
                        .delete()
                        .await
                        .wrap_context("deleting snapshot metadata")?;
                    to_delete
                        .advance(1)
                        .await
                        .wrap_context("moving to next to-delete snapshot")?;
                }

                // Write the new creation snapshot data
                let object_js = serde_wasm_bindgen::to_value(&object)
                    .wrap_context("serializing new creation snapshot")?;
                snapshots
                    .add_kv(&new_created_at_js, &object_js)
                    .await
                    .wrap_context("saving new creation snapshot data")?;

                // And the new creation snapshot metadata
                let new_creation_snapshot_meta = SnapshotMeta {
                    snapshot_id: new_created_at,
                    type_id: *T::type_ulid(),
                    object_id,
                    is_creation: Some(1),
                    is_latest: new_creation_should_be_latest.then(|| 1),
                    normalizer_version: fts::normalizer_version(),
                    snapshot_version: T::snapshot_version(),
                    is_locked: force_lock.then(|| 1).or(creation_meta.is_locked),
                    required_binaries: required_binaries.clone(),
                };
                let new_creation_snapshot_meta_js =
                    serde_wasm_bindgen::to_value(&new_creation_snapshot_meta)
                        .wrap_context("serializing snapshot metadata")?;
                snapshots_meta
                    .put(&new_creation_snapshot_meta_js)
                    .await
                    .wrap_context("saving the new creation snapshot metadata")?;

                // Re-compute the latest snapshot if needed
                if !new_creation_should_be_latest {
                    let mut last_applied_event_id = new_created_at;
                    let mut_object = Arc::make_mut(&mut object);
                    // Read all the events one by one, applying them to the object
                    let mut to_apply = object_event
                        .cursor()
                        .range(
                            &**Array::from_iter([&object_id_js, &zero_id])
                                ..=&**Array::from_iter([&object_id_js, &max_id]),
                        )
                        .wrap_context("limiting the range of events to apply")?
                        .open_key()
                        .await
                        .wrap_context("opening the cursor on events to apply")?;
                    while let Some(event_id_js) = to_apply.primary_key() {
                        let event_js = events
                            .get(&event_id_js)
                            .await
                            .wrap_context(
                                "retrieving data for an event for which we have the metadata",
                            )?
                            .ok_or_else(|| {
                                crate::Error::Other(anyhow!(
                                    "had no event data for an event with known metadata"
                                ))
                            })?;
                        let event_id = serde_wasm_bindgen::from_value::<EventId>(event_id_js)
                            .wrap_context("deserializing event id")?;
                        last_applied_event_id = event_id;
                        let event = serde_wasm_bindgen::from_value::<T::Event>(event_js)
                            .wrap_context("deserializing event data")?;
                        mut_object.apply(DbPtr::from(object_id), &event);
                        to_apply
                            .advance(1)
                            .await
                            .wrap_context("advancing events-to-apply cursor")?;
                    }

                    // Write the metadata
                    let new_latest_snapshot_meta = SnapshotMeta {
                        snapshot_id: last_applied_event_id,
                        type_id: *T::type_ulid(),
                        object_id,
                        is_creation: None,
                        is_latest: Some(1),
                        normalizer_version: fts::normalizer_version(),
                        snapshot_version: T::snapshot_version(),
                        is_locked: None,
                        required_binaries: object.required_binaries(),
                    };
                    let new_latest_snapshot_meta_js =
                        serde_wasm_bindgen::to_value(&new_latest_snapshot_meta)
                            .wrap_context("serializing snapshot metadata")?;
                    snapshots_meta
                        .put(&new_latest_snapshot_meta_js)
                        .await
                        .wrap_context("saving the new latest snapshot metadata")?;

                    // And the data
                    let last_applied_event_id_js = last_applied_event_id.to_js_string();
                    let object_js = serde_wasm_bindgen::to_value(&object)
                        .wrap_context("serializing new latest snapshot")?;
                    snapshots
                        .add_kv(&last_applied_event_id_js, &object_js)
                        .await
                        .wrap_context("saving new latest snapshot data")?;
                }

                // And finally, validate the required binaries
                check_required_binaries(binaries, required_binaries).await?;

                Ok(Some(object))
            })
            .await
            .wrap_with_context(|| {
                format!("recreating {object_id:?} with new data from {new_created_at:?}")
            })
    }

    async fn remove(&self, object_id: ObjectId) -> crate::Result<()> {
        let object_id_js = object_id.to_js_string();
        let zero_id = EventId::from_u128(0).to_js_string();
        let max_id = EventId::from_u128(u128::MAX).to_js_string();

        self.db
            .transaction(&["snapshots", "snapshots_meta", "events", "events_meta"])
            .rw()
            .run(move |transaction| async move {
                let snapshots_meta = transaction
                    .object_store("snapshots_meta")
                    .wrap_context("retrieving the 'snapshots_meta' object store")?;
                let events_meta = transaction
                    .object_store("events_meta")
                    .wrap_context("retrieving the 'events_meta' object store")?;
                let snapshots = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving the 'snapshots' object store")?;
                let events = transaction
                    .object_store("events")
                    .wrap_context("retrieving the 'events' object store")?;

                let creation_object = snapshots_meta
                    .index("creation_object")
                    .wrap_context("retrieving the 'creation_object' index")?;
                let object_snapshot = snapshots_meta
                    .index("object_snapshot")
                    .wrap_context("retrieving the 'object_snapshot' index")?;

                let object_event = events_meta
                    .index("object_event")
                    .wrap_context("retrieving the 'object_event' index")?;

                // Check we're good to delete this object
                let object_does_exist = creation_object
                    .contains(&Array::from_iter([&JsValue::from(1), &object_id_js]))
                    .await
                    .wrap_context("retrieving creation snapshot")?;
                if !object_does_exist {
                    // Object already does not exist, so the removal already succeeded
                    return Ok(());
                };

                // We're good to go, delete everything
                let mut to_remove = object_snapshot
                    .cursor()
                    .range(
                        &**Array::from_iter([&object_id_js, &zero_id])
                            ..=&**Array::from_iter([&object_id_js, &max_id]),
                    )
                    .wrap_context("limiting to-delete range")?
                    .open()
                    .await
                    .wrap_context("opening to-delete cursor")?;
                while let Some(snapshot_id_js) = to_remove.primary_key() {
                    snapshots
                        .delete(&snapshot_id_js)
                        .await
                        .wrap_context("deleting snapshot data")?;
                    to_remove
                        .delete()
                        .await
                        .wrap_context("deleting snapshot metadata")?;
                    to_remove
                        .advance(1)
                        .await
                        .wrap_context("going to next to-delete snapshot")?;
                }

                let mut to_remove = object_event
                    .cursor()
                    .range(
                        &**Array::from_iter([&object_id_js, &zero_id])
                            ..=&**Array::from_iter([&object_id_js, &max_id]),
                    )
                    .wrap_context("limiting to-delete range")?
                    .open()
                    .await
                    .wrap_context("opening to-delete cursor")?;
                while let Some(event_id_js) = to_remove.primary_key() {
                    events
                        .delete(&event_id_js)
                        .await
                        .wrap_context("deleting event data")?;
                    to_remove
                        .delete()
                        .await
                        .wrap_context("deleting event metadata")?;
                    to_remove
                        .advance(1)
                        .await
                        .wrap_context("going to next to-delete event")?;
                }

                Ok(())
            })
            .await
            .wrap_with_context(|| format!("removing {object_id:?}"))
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<[u8]>) -> crate::Result<()> {
        if crate::hash_binary(&data) != binary_id {
            return Err(crate::Error::BinaryHashMismatch(binary_id));
        }
        let ary = Uint8Array::new_with_length(u32::try_from(data.len()).unwrap());
        ary.copy_from(&data);
        let res = self
            .db
            .transaction(&["binaries"])
            .rw()
            .run(move |transaction| async move {
                let binaries = transaction
                    .object_store("binaries")
                    .wrap_context("retrieving the 'binaries' object store")?;

                binaries
                    .put_kv(&binary_id.to_js_string(), &ary)
                    .await
                    .wrap_context("writing binary")?;

                Ok(())
            })
            .await
            .wrap_with_context(|| format!("writing {binary_id:?}"));
        if res.is_ok() {
            self.objects_unlocked_this_run
                .set(self.objects_unlocked_this_run.get() + 1);
        }
        res
    }

    async fn get_binary(&self, binary_id: BinPtr) -> crate::Result<Option<Arc<[u8]>>> {
        let ary = self
            .db
            .transaction(&["binaries"])
            .run(move |transaction| async move {
                let binaries = transaction
                    .object_store("binaries")
                    .wrap_context("retrieving the 'binaries' object store")?;

                binaries.get(&binary_id.to_js_string()).await
            })
            .await
            .wrap_with_context(|| format!("fetching {binary_id:?}"))?;
        let Some(ary) = ary else {
            return Ok(None);
        };
        let ary = ary
            .dyn_into::<Uint8Array>()
            .wrap_context("recovering Uint8Array from stored data")?;
        Ok(Some(ary.to_vec().into_boxed_slice().into()))
    }
}

async fn check_required_binaries(
    binaries_store: indexed_db::ObjectStore<crate::Error>,
    binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    let missing_binaries = future::try_join_all(binaries.iter().map(|&b| {
        binaries_store
            .contains(&b.to_js_string())
            .map_ok(move |present| (!present).then_some(b))
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
