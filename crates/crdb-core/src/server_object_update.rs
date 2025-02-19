use crate::{ReadPermsChanges, UpdatesWithSnap, User};

pub struct ServerObjectUpdate {
    pub updates: UpdatesWithSnap,

    /// List of all the users who were able to read the object both before and after the update.
    /// Their permissions have not changed.
    pub unchanged_readers: Vec<User>,

    /// List of all the users who gained or lost read access to the affected object.
    pub read_perms_changes: Vec<ReadPermsChanges>,
}
