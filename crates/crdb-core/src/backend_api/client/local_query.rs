use crate::{ObjectId, Query, TypeId};

pub trait LocalQuery: 'static + waaaa::Send + waaaa::Sync {
    // TODO(api-med): the resulting `Vec` should be a stream
    fn local_query(
        &self,
        type_id: TypeId,
        query: &Query,
    ) -> impl waaaa::Future<Output = crate::Result<Vec<ObjectId>>>;
}
