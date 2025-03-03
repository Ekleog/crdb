use crate::{ObjectId, Query, TypeId, Updatedness, User};

pub trait ServerQuery: 'static + waaa::Send + waaa::Sync {
    // TODO(api-med): replace Vec with Stream
    fn server_query(
        &self,
        user: User,
        type_id: TypeId,
        only_updated_since: Option<Updatedness>,
        query: &Query,
    ) -> impl waaa::Future<Output = crate::Result<Vec<ObjectId>>>;
}
