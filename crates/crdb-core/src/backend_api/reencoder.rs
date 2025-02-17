use crate::Object;

pub trait Reencoder: 'static + waaaa::Send + waaaa::Sync {
    /// Returns the number of errors that happened while re-encoding
    fn reencode_old_versions<T: Object>(&self) -> impl waaaa::Future<Output = usize>;
}
