use crate::{
    db_trait::Db, error::ResultExt, full_object::FullObject, BinPtr, CanDoCallbacks, CrdbStream,
    EventId, Object, ObjectId, Query, Timestamp, User,
};
use anyhow::anyhow;
use indexed_db_futures::{prelude::*, IdbDatabase};
use std::sync::Arc;
use wasm_bindgen_futures::JsFuture;

pub struct IndexedDb {
    is_persistent: bool,
    db: IdbDatabase,
}

impl IndexedDb {
    pub async fn connect(url: &str) -> anyhow::Result<IndexedDb> {
        let is_persistent = JsFuture::from(
            web_sys::window()
                .ok_or_else(|| anyhow!("not running in a browser"))?
                .navigator()
                .storage()
                .persist()
                .map_err(|_| {
                    anyhow!(
                        "failed to request persistence, did the user disable storage altogether?"
                    )
                })?,
        )
        .await
        .map_err(|_| anyhow!("failed to resolve request for persistence"))?
        .as_bool()
        .ok_or_else(|| anyhow!("requesting for persistence did not return a boolean"))?;

        let mut db_req = IdbDatabase::open_u32(url, 1).expect("1 is greater than 0");
        db_req.set_on_upgrade_needed(Some(|evt: &IdbVersionChangeEvent| {
            let db = evt.db();
            db.create_object_store("snapshots")?;
            db.create_object_store("events")?;
            db.create_object_store("binaries")?;
            Ok(())
        }));
        let db = db_req.await.wrap_context("failed creating the database")?;

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
        let transaction = self
            .db
            .transaction_on_multi_with_mode(
                &["snapshots", "events", "binaries"],
                IdbTransactionMode::Readwrite,
            )
            .wrap_context("obtaining transaction into IndexedDb database")?;
        let snapshots = transaction
            .object_store("snapshots")
            .wrap_context("obtaining 'snapshots' object store from transaction")?;
        let add_attempt = snapshots.add_key_val_owned(
            object_id.to_js_string(),
            &serde_wasm_bindgen::to_value(&*object).wrap_context("serializing object to json")?,
        );
        match add_attempt {
            Ok(res) => res
                .await
                .wrap_with_context(|| format!("completing database addition of {object_id:?}"))?,
            Err(add_attempt_error) => {
                let already_existing = snapshots
                    .get(&object_id.to_js_string())
                    .wrap_with_context(|| format!("requesting {object_id:?}"))?
                    .await
                    .wrap_with_context(|| format!("fetching {object_id:?}"))?;
                let Some(already_existing) = already_existing else {
                    return Err(add_attempt_error)
                        .wrap_with_context(|| format!("adding {object_id:?} to database"));
                };
                let Ok(already_existing) = serde_wasm_bindgen::from_value::<T>(already_existing)
                else {
                    return Err(crate::Error::ObjectAlreadyExists(object_id));
                };
                if already_existing != *object {
                    return Err(crate::Error::ObjectAlreadyExists(object_id));
                }
                return Ok(());
            }
        }
        let event_already_exists = transaction
            .object_store("events")
            .wrap_context("obtaining 'events' object store from transaction")?
            .get(&created_at.to_js_string())
            .wrap_with_context(|| format!("requesting {created_at:?}"))?
            .await
            .wrap_with_context(|| format!("fetching {created_at:?}"))?
            .is_some();
        if event_already_exists {
            transaction.abort().wrap_with_context(|| {
                format!("failed aborting transaction creating {object_id:?}")
            })?;
            return Err(crate::Error::EventAlreadyExists(created_at));
        }
        match check_required_binaries(&transaction, object.required_binaries()).await {
            Ok(()) => Ok(()),
            Err(e) => {
                transaction.abort().wrap_with_context(|| {
                    format!("failed aborting transaction creating {object_id:?}")
                })?;
                Err(e).wrap_with_context(|| {
                    format!("checking that {object_id:?} has all required binaries")
                })
            }
        }
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
    transaction: &IdbTransaction<'_>,
    binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    let binaries_store = transaction
        .object_store("binaries")
        .wrap_context("opening 'binaries' object store")?;
    let mut missing_binaries = Vec::new();
    for b in binaries {
        if binaries_store
            .get_key(&b.to_js_string())
            .wrap_with_context(|| format!("requesting whether {b:?} is present"))?
            .await
            .wrap_with_context(|| format!("checking whether {b:?} is present"))?
            .is_none()
        {
            missing_binaries.push(b);
        }
    }
    if !missing_binaries.is_empty() {
        return Err(crate::Error::MissingBinaries(missing_binaries));
    }
    Ok(())
}
