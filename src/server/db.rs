use crate::{EventId, ObjectId, Timestamp, TypeId};
use anyhow::Context;

pub(crate) struct Db {
    _db: sqlx::PgPool,
    new_object_cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    new_event_cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
}
// TODO: impl (Can)ApplyCallbacks for Db

impl Db {
    pub async fn connect(db_url: &str) -> anyhow::Result<Db> {
        Ok(Db {
            _db: sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // TODO: make configurable (builder pattern?)
                .connect(&db_url)
                .await
                .with_context(|| format!("opening database {db_url:?}"))?,
            new_object_cb: Box::new(|_, _, _, _| ()),
            new_event_cb: Box::new(|_, _, _, _, _| ()),
        })
    }
}

impl crate::Db for Db {
    fn set_new_object_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    ) {
        self.new_object_cb = cb;
    }

    fn set_new_event_cb(
        &mut self,
        cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
    ) {
        self.new_event_cb = cb;
    }

    async fn create<T: crate::Object>(
        &self,
        time: crate::Timestamp,
        object_id: ObjectId,
        object: crate::MaybeParsed<T>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get<T: crate::Object>(&self, ptr: ObjectId) -> anyhow::Result<crate::MaybeParsed<T>> {
        todo!()
    }

    async fn submit<T: crate::Object>(
        &self,
        time: crate::Timestamp,
        object: ObjectId,
        event_id: EventId,
        event: crate::MaybeParsed<T::Event>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn snapshot(&self, time: crate::Timestamp, object: ObjectId) -> anyhow::Result<()> {
        todo!()
    }
}
