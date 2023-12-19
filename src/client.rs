use crate::{DbPtr, Object};
use std::sync::Arc;

pub trait Db {
    fn create<T: Object>(&self, object: T) -> anyhow::Result<DbPtr<T>>;
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
    fn submit<T: Object>(&self, object: DbPtr<T>, event: T::Event) -> anyhow::Result<()>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    () => {
        // TODO: generate a Db impl (or maybe it doesn't even need to be a trait?)
    };
}
