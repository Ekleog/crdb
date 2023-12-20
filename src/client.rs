use crate::{DbPtr, Object};
use std::sync::Arc;
use uuid::Uuid;

trait Db {
    fn set_new_object_cb(&mut self, cb: Box<dyn FnMut(Uuid, serde_json::Value)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn FnMut(Uuid, serde_json::Value)>);

    fn create<T: Object>(&self, object: T) -> anyhow::Result<DbPtr<T>> {
        self.create_unchecked(Uuid::new_v4(), object)
    }
    fn create_unchecked<T: Object>(&self, object_id: Uuid, object: T) -> anyhow::Result<DbPtr<T>>;
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
    fn submit<T: Object>(
        &self,
        object: DbPtr<T>,
        event_id: Uuid,
        event: T::Event,
    ) -> anyhow::Result<()>;
    fn snapshot<T: Object>(&self, object: DbPtr<T>) -> anyhow::Result<()>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    () => {
        // TODO: generate an "impl" of client::Db that just forwards to the crate::Db impl of Cache<Api>
        // set_new_* MUST be replaced by one function for each object/event type, so that the user can properly handle them.
    };
}
