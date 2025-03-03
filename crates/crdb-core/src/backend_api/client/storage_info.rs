use crate::ClientStorageInfo;

pub trait StorageInfo: 'static + waaa::Send + waaa::Sync {
    fn storage_info(&self) -> impl waaa::Future<Output = crate::Result<ClientStorageInfo>>;
}
