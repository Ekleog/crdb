use crate::{DbPtr, JsonSnapshot, Object, ObjectId};
use std::sync::Arc;

pub trait ObjectGet: 'static + waaa::Send + waaa::Sync {
    fn get<T: Object>(
        &self,
        ptr: DbPtr<T>,
    ) -> impl '_ + waaa::Future<Output = crate::Result<Arc<T>>> {
        async move {
            let json = self.get_json(ptr.to_object_id()).await?;
            Ok(Arc::new(json.into_parsed()?))
        }
    }

    fn get_json(&self, ptr: ObjectId) -> impl waaa::Future<Output = crate::Result<JsonSnapshot>>;
}
