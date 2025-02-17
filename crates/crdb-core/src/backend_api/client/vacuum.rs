use crate::{CrdbSyncFn, ObjectId, QueryId};

pub trait Vacuum: 'static + waaaa::Send + waaaa::Sync {
    fn client_vacuum(
        &self,
        notify_removals: impl 'static + CrdbSyncFn<ObjectId>,
        notify_query_removals: impl 'static + CrdbSyncFn<QueryId>,
    ) -> impl waaaa::Future<Output = crate::Result<()>>;
}
