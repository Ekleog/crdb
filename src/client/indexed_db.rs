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
    is_creation: bool,
    is_latest: bool,
    snapshot_version: i32,
    is_locked: bool,
    upload_succeeded: bool,
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
                snapshots_meta
                    .build_index("latest_object", &["is_latest", "object_id"])
                    .create()?;
                snapshots_meta
                    .build_index("creation_object", &["is_creation", "object_id"])
                    .create()?;
                snapshots_meta
                    .build_index("object_snapshot", &["object_id", "snapshot_id"])
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

#[allow(unused_variables)] // TODO: remove
impl Db for IndexedDb {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> crate::Result<()> {
        let new_snapshot_meta = SnapshotMeta {
            snapshot_id: created_at,
            type_id: *T::type_ulid(),
            object_id,
            is_creation: true,
            is_latest: true,
            snapshot_version: T::snapshot_version(),
            is_locked: false, // TODO: allow for atomic create-and-lock
            upload_succeeded: false,
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

                // First, try inserting the object
                match snapshots_meta.add(&new_snapshot_meta_js).await {
                    Err(indexed_db::Error::AlreadyExists) => {
                        // Snapshot metadata already exists. Check that the already-existing value was the same
                        let Some(old_meta_js) = snapshots_meta
                            .index("creation_object")
                            .wrap_context("retrieving 'creation_object' index")?
                            .get(&[&JsValue::TRUE, &object_id_js])
                            .await
                            .wrap_with_context(|| {
                                format!("getting {object_id:?}'s creation snapshot")
                            })?
                        else {
                            // Object did not exist yet, it means it was an event conflict
                            return Err(crate::Error::EventAlreadyExists(created_at).into());
                        };
                        let mut old_meta =
                            serde_wasm_bindgen::from_value::<SnapshotMeta>(old_meta_js)
                                .wrap_with_context(|| {
                                    format!(
                                    "deserializing preexisting snapshot metadata for {object_id:?}"
                                )
                                })?;
                        // Ignore a few fields in comparison below
                        old_meta.is_latest = true;
                        old_meta.is_locked = false;
                        old_meta.upload_succeeded = false;
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
                            Err(crate::Error::ObjectAlreadyExists(object_id).into())
                        } else {
                            Ok(())
                        }
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
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        todo!()
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
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
