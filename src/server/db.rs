use crate::traits::{EventId, ObjectId, Timestamp, TypeId};
use anyhow::Context;

pub(crate) struct Db {
    _db: sqlx::PgPool,
    _new_object_cb: Box<dyn Fn(Timestamp, ObjectId, TypeId, serde_json::Value)>,
    _new_event_cb: Box<dyn Fn(Timestamp, ObjectId, EventId, TypeId, serde_json::Value)>,
}
// TODO: impl (Can)ApplyCallbacks for Db

impl Db {
    pub async fn connect(db_url: &str) -> anyhow::Result<Db> {
        // TODO: switch to surrealdb?
        Ok(Db {
            _db: sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // TODO: make configurable (builder pattern?)
                .connect(&db_url)
                .await
                .with_context(|| format!("opening database {db_url:?}"))?,
            _new_object_cb: Box::new(|_, _, _, _| ()),
            _new_event_cb: Box::new(|_, _, _, _, _| ()),
        })
    }
}
