use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CanDoCallbacks, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use anyhow::anyhow;
use indexed_db_futures::{prelude::*, IdbDatabase};
use std::sync::Arc;
use wasm_bindgen_futures::JsFuture;

pub struct IndexedDb {
    is_persistent: bool,
    _db: IdbDatabase,
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
        let db = db_req
            .await
            .map_err(|_| anyhow!("failed creating the database"))?;

        Ok(IndexedDb {
            is_persistent,
            _db: db,
        })
    }

    pub fn is_persistent(&self) -> bool {
        self.is_persistent
    }
}

#[allow(unused_variables)] // TODO: remove
impl Db for IndexedDb {
    async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        // todo!()
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        // todo!()
        futures::stream::empty()
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        todo!()
    }

    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
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
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
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

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}
