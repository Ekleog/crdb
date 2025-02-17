use crate::{DbPtr, Object};
use std::sync::Arc;

pub trait ObjectGet: 'static + waaaa::Send + waaaa::Sync {
    fn get<T: Object>(
        &self,
        ptr: DbPtr<T>,
    ) -> impl '_ + waaaa::Future<Output = crate::Result<Arc<T>>>;
}
