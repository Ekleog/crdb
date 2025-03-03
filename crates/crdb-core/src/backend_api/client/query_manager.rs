use crate::{ClientSavedQueryMeta, Importance, ObjectId, Query, QueryId, TypeId, Updatedness};
use std::{collections::HashSet, sync::Arc};

pub trait QueryManager: 'static + waaa::Send + waaa::Sync {
    // TODO(api-med): turn the Vec into a Stream
    fn list_saved_queries(
        &self,
    ) -> impl waaa::Future<Output = crate::Result<Vec<ClientSavedQueryMeta>>>;

    fn query_register(
        &self,
        query_id: QueryId,
        type_id: TypeId,
        query: Arc<Query>,
        importance: Importance,
    ) -> impl waaa::Future<Output = crate::Result<()>>;

    fn query_update(
        &self,
        queries: &HashSet<QueryId>,
        now_have_all_until: Updatedness,
    ) -> impl waaa::Future<Output = crate::Result<()>>;

    // TODO(api-med): take a Stream instead of a Vec
    fn query_forget(
        &self,
        query_id: QueryId,
        objects_matching_query: Vec<ObjectId>,
    ) -> impl waaa::Future<Output = crate::Result<()>>;

    // TODO(api-med): take a Stream instead of a Vec
    fn set_query_importance(
        &self,
        query_id: QueryId,
        importance: Importance,
        objects_matching_query: Vec<ObjectId>,
    ) -> impl waaa::Future<Output = crate::Result<()>>;
}
