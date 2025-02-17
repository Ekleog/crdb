use crate::{EventId, Update, User};
use std::collections::HashSet;
use web_time::SystemTime;

pub trait Vacuum: 'static + waaaa::Send + waaaa::Sync {
    /// Clean up and optimize the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    fn server_vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        kill_sessions_older_than: Option<SystemTime>,
        notify_recreation: impl FnMut(Update, HashSet<User>),
    ) -> impl std::future::Future<Output = crate::Result<()>>;
}
