use std::sync::Arc;

use crate::Update;

// Each update is both the list of updates itself, and the new latest snapshot
// for query matching, available if the latest snapshot actually changed. Also,
// the list of users allowed to read this object.
// TODO(api-highest): this is only used server-side internally, move it there?
#[derive(Debug)]
pub struct UpdatesWithSnap {
    // The list of actual updates
    pub updates: Vec<Arc<Update>>,

    // The new last snapshot, if the update did change it (ie. no vacuum) and if the users affected
    // actually do have access to it. This is used for query matching.
    // It is always on the latest snapshot version
    pub new_last_snapshot: Option<Arc<serde_json::Value>>,
}
