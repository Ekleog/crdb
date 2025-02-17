use crate::{ObjectId, Query, TypeId, Updatedness, User};

pub trait ServerQuery: 'static + waaaa::Send + waaaa::Sync {
    // TODO(api-med): replace Vec with Stream
    fn server_query(
        &self,
        user: User,
        type_id: TypeId,
        only_updated_since: Option<Updatedness>,
        query: &Query,
    ) -> impl waaaa::Future<Output = crate::Result<Vec<ObjectId>>>;
}
