use crate::{CanDoCallbacks, CrdbFuture, EventId, Object, ObjectId, Updatedness, User};
use std::{collections::HashSet, sync::Arc};

// TODO(blocked): replace with an associated type of ServerSideDb once https://github.com/rust-lang/rust/pull/120700 stabilizes
pub type ComboLock<'a> = (
    reord::Lock,
    <lockable::LockPool<ObjectId> as lockable::Lockable<ObjectId, ()>>::Guard<'a>,
);

pub trait ServerSideDb {
    fn get_users_who_can_read<'a, 'ret: 'a, T: Object, C: CanDoCallbacks>(
        &'ret self,
        object_id: ObjectId,
        object: &'a T,
        cb: &'a C,
    ) -> impl 'a
           + CrdbFuture<Output = anyhow::Result<(HashSet<User>, Vec<ObjectId>, Vec<ComboLock<'ret>>)>>;

    /// This function assumes that the lock on `object_id` is already taken
    ///
    /// Returns `Some` iff the object actually changed
    fn recreate_at<'ret, 'a: 'ret, T: Object, C: CanDoCallbacks>(
        &'ret self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> impl 'ret + CrdbFuture<Output = crate::Result<Option<(EventId, Arc<T>)>>>;
}
