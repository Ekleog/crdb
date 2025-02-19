use std::collections::HashSet;

use crate::{ObjectId, ServerObjectLock, User};

pub struct UsersWhoCanRead<'a> {
    pub users: HashSet<User>,
    pub depends_on: Vec<ObjectId>,
    pub locks: Vec<ServerObjectLock<'a>>,
}
