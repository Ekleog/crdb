use crate::{DbPtr, Object};
use std::sync::Arc;
use ulid::Ulid;

trait Db {
    fn set_new_object_cb(&mut self, cb: Box<dyn FnMut(Ulid, serde_json::Value)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn FnMut(Ulid, serde_json::Value)>);

    fn create_unchecked<T: Object>(&self, object_id: Ulid, object: T) -> anyhow::Result<DbPtr<T>>;
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
    fn submit_unchecked<T: Object>(
        &self,
        object: DbPtr<T>,
        event_id: Ulid,
        event: T::Event,
    ) -> anyhow::Result<()>;

    /// `at_time` is the number of milliseconds since unix epoch, at which the object should be snapshotted
    fn snapshot<T: Object>(&self, object: DbPtr<T>, at_time: u64) -> anyhow::Result<()>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ($($t:tt)*) => {
        // TODO: generate an "impl" of client::Db that just forwards to the crate::Db impl of Cache<Api>
        // set_new_* MUST be replaced by one function for each object/event type, so that the user can properly handle them.
    };
}
