use std::sync::Arc;

use crate::{Importance, Query, QueryId, TypeId, Updatedness};

pub struct ClientSavedQueryMeta {
    pub query_id: QueryId,
    pub type_id: TypeId,
    pub query: Arc<Query>,
    pub have_all_until: Option<Updatedness>,
    pub importance: Importance,
}
