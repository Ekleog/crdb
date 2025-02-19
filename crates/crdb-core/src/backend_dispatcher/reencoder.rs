use crate::backend_api;

use super::private;

pub trait Reencoder: private::Sealed {
    /// Returns the number of errors that happened while re-encoding
    fn reencode_old_versions<D: backend_api::Reencoder>(
        call_on: &D,
    ) -> impl '_ + waaaa::Future<Output = usize>;
}
