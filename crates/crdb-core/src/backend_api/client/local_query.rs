use crate::{ObjectId, Query, TypeId};

pub trait LocalQuery: 'static + waaa::Send + waaa::Sync {
    // TODO(api-med): the resulting `Vec` should be a stream
    fn local_query(
        &self,
        type_id: TypeId,
        query: &Query,
    ) -> impl waaa::Future<Output = crate::Result<Vec<ObjectId>>>;
}
