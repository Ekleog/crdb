use crate::ClientStorageInfo;

pub trait StorageInfo: 'static + waaaa::Send + waaaa::Sync {
    fn storage_info(&self) -> impl waaaa::Future<Output = crate::Result<ClientStorageInfo>>;
}
