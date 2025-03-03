use crate::{CrdbSyncFn, ObjectId, QueryId};

pub trait Vacuum: 'static + waaa::Send + waaa::Sync {
    fn client_vacuum(
        &self,
        notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
    ) -> impl waaa::Future<Output = crate::Result<()>>;
}
