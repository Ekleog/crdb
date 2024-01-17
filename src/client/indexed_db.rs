use crate::{
    db_trait::Db, error::ResultExt, full_object::FullObject, BinPtr, CanDoCallbacks, CrdbStream,
    EventId, Object, ObjectId, Query, Timestamp, TypeId, User,
};
use anyhow::anyhow;
use futures::{future, TryFutureExt};
use std::sync::Arc;
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
                    .key_path(&["snapshot_id"])
                    .create()?;
                let events_meta = db
                    .build_object_store("events_meta")
                    .key_path(&["event_id"])
                    .create()?;

                snapshots_meta
                    .build_index("latest_object", &["is_latest", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_index("creation_object", &["is_creation", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_index("locked_object", &["is_locked", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_index("not_uploaded_object", &["upload_not_over", "object_id"])
                    .unique()
                    .create()?;
                snapshots_meta
                    .build_index("object_snapshot", &["object_id", "snapshot_id"])
                    .create()?;

                events_meta
                    .build_index("not_uploaded_event", &["upload_not_over", "event_id"])
                    .unique()
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
                    .get(&[&JsValue::from(1), &object_id_js])
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
                    let old_data = serde_wasm_bindgen::from_value::<T>(old_data_js)
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
        let object_id_js = object_id.to_js_string();

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

                // Check the object does exist, is of the right type and is not too new
                let Some(creation_snapshot_js) = creation_object
                    .get(&[&JsValue::from(1), &object_id_js])
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

                // Insert the event itself
                let event_js = serde_wasm_bindgen::to_value(&*event)
                    .wrap_with_context(|| format!("serializing {event_id:?}"))?;

                Ok(())
            })
            .await
            .wrap_with_context(|| {
                format!("running submission creation for {event_id:?} on {object_id:?}")
            })
    }

    async fn get<T: Object>(&self, _object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
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
