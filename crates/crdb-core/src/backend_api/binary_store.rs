use crate::BinPtr;
use std::sync::Arc;

pub trait BinaryStore: 'static + waaa::Send + waaa::Sync {
    fn binary_create(
        &self,
        binary_id: BinPtr,
        data: Arc<[u8]>,
    ) -> impl waaa::Future<Output = crate::Result<()>>;

    fn binary_get(
        &self,
        binary_id: BinPtr,
    ) -> impl waaa::Future<Output = crate::Result<Option<Arc<[u8]>>>>;

    fn binary_delete(&self, binary_id: BinPtr) -> impl waaa::Future<Output = crate::Result<()>>;
}
