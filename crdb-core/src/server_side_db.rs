use crate::{CanDoCallbacks, CrdbFuture, Object, ObjectId, User};
use std::collections::HashSet;

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
}
