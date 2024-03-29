use web_time::SystemTime;

use crate::{
    CanDoCallbacks, Db, EventId, Object, ObjectData, ObjectId, Query, TypeId, Update, Updatedness,
    User,
};
use std::{collections::HashSet, pin::Pin, sync::Arc};

// TODO(blocked): replace with an associated type of ServerSideDb once https://github.com/rust-lang/rust/pull/120700 stabilizes
// This will allow removing the `reord` and `lockable` deps.
pub type ComboLock<'a> = (
    reord::Lock,
    <lockable::LockPool<ObjectId> as lockable::Lockable<ObjectId, ()>>::Guard<'a>,
);

pub struct ReadPermsChanges {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    pub lost_read: HashSet<User>,
    pub gained_read: HashSet<User>,
}

pub trait ServerSideDb: 'static + waaaa::Send + waaaa::Sync + Db {
    type Transaction: waaaa::Send;

    // TODO(blocked): replace with -> impl once https://github.com/rust-lang/rust/issues/100013 is fixed
    // This will also remove the clippy lint
    #[allow(clippy::type_complexity)]
    fn get_users_who_can_read<'a, 'ret: 'a, T: Object, C: CanDoCallbacks>(
        &'ret self,
        object_id: ObjectId,
        object: &'a T,
        cb: &'a C,
    ) -> Pin<
        Box<
            dyn 'a
                + waaaa::Future<
                    Output = anyhow::Result<(HashSet<User>, Vec<ObjectId>, Vec<ComboLock<'ret>>)>,
                >,
        >,
    >;

    // TODO(test-high): introduce in server-side fuzzer
    fn get_all(
        &self,
        transaction: &mut Self::Transaction,
        user: User,
        object_id: ObjectId,
        only_updated_since: Option<Updatedness>,
    ) -> impl waaaa::Future<Output = crate::Result<ObjectData>>;

    fn server_query(
        &self,
        user: User,
        type_id: TypeId,
        only_updated_since: Option<Updatedness>,
        query: Arc<Query>,
    ) -> impl waaaa::Future<Output = crate::Result<Vec<ObjectId>>>;

    /// Cleans up and optimizes up the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    fn server_vacuum(
        &self,
        no_new_changes_before: Option<EventId>,
        updatedness: Updatedness,
        kill_sessions_older_than: Option<SystemTime>,
        notify_recreation: impl FnMut(Update, HashSet<User>),
    ) -> impl std::future::Future<Output = crate::Result<()>>;

    /// This function assumes that the lock on `object_id` is already taken
    ///
    /// Returns `Some` iff the object actually changed
    fn recreate_at<'a, T: Object, C: CanDoCallbacks>(
        &'a self,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> impl 'a + waaaa::Future<Output = crate::Result<Option<(EventId, Arc<T>)>>>;

    fn create_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        updatedness: Updatedness,
    ) -> impl '_ + waaaa::Future<Output = crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>>>;

    fn submit_and_return_rdep_changes<T: Object>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        updatedness: Updatedness,
    ) -> impl '_ + waaaa::Future<Output = crate::Result<Option<(Arc<T>, Vec<ReadPermsChanges>)>>>;
}
