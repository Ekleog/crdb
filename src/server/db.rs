use anyhow::Context;
use uuid::Uuid;

pub(crate) struct Db {
    _db: sqlx::PgPool,
    new_object_cb: Box<dyn Fn(Uuid, serde_json::Value)>,
    new_event_cb: Box<dyn Fn(Uuid, Uuid, serde_json::Value)>,
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
            new_object_cb: Box::new(|_, _| ()),
            new_event_cb: Box::new(|_, _, _| ()),
        })
    }
}

impl crate::Db for Db {
    fn set_new_object_cb(&mut self, cb: Box<dyn Fn(Uuid, serde_json::Value)>) {
        self.new_object_cb = cb;
    }

    fn set_new_event_cb(&mut self, cb: Box<dyn Fn(Uuid, Uuid, serde_json::Value)>) {
        self.new_event_cb = cb;
    }

    fn create(&self, object_id: Uuid, object: serde_json::Value) -> anyhow::Result<()> {
        todo!()
    }

    fn get(&self, ptr: Uuid) -> anyhow::Result<serde_json::Value> {
        todo!()
    }

    fn submit(&self, object: Uuid, event_id: Uuid, event: serde_json::Value) -> anyhow::Result<()> {
        todo!()
    }

    fn snapshot(&self, object: Uuid) -> anyhow::Result<()> {
        todo!()
    }
}
