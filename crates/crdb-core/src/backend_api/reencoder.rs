use crate::Object;

pub trait Reencoder: 'static + waaa::Send + waaa::Sync {
    /// Returns the number of errors that happened while re-encoding
    fn reencode_old_versions<T: Object>(&self) -> impl waaa::Future<Output = usize>;
}
