use crate::{BinPtr, Upload, UploadId};

pub trait UploadQueue: 'static + waaa::Send + waaa::Sync {
    // TODO(api-med): return a Stream instead of a Vec
    fn upload_list(&self) -> impl waaa::Future<Output = crate::Result<Vec<UploadId>>>;

    fn upload_get(
        &self,
        upload_id: UploadId,
    ) -> impl waaa::Future<Output = crate::Result<Option<Upload>>>;

    fn upload_enqueue(
        &self,
        upload: Upload,
        required_binaries: Vec<BinPtr>,
    ) -> impl waaa::Future<Output = crate::Result<UploadId>>;

    fn upload_finished(&self, upload_id: UploadId)
        -> impl waaa::Future<Output = crate::Result<()>>;
}
