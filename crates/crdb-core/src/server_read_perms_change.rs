use std::collections::HashSet;

use crate::{ObjectId, TypeId, User};

pub struct ReadPermsChanges {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    pub lost_read: HashSet<User>,
    pub gained_read: HashSet<User>,
}
