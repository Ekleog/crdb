use crate::{
    api::{BinPtr, Query},
    Object, User,
};
use anyhow::{anyhow, Context};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, future::Future, ops::Bound, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ObjectId(pub(crate) Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EventId(pub(crate) Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TypeId(pub(crate) Ulid);

macro_rules! impl_for_id {
    ($type:ty) => {
        impl $type {
            #[allow(dead_code)]
            pub(crate) fn time(&self) -> Timestamp {
                Timestamp(self.0.timestamp_ms())
            }
        }
    };
}

impl_for_id!(ObjectId);
impl_for_id!(EventId);
impl_for_id!(TypeId);

#[derive(Clone)]
pub(crate) struct Change {
    event: Arc<dyn Any + Send + Sync>,
    snapshot_after: Option<Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone)]
pub(crate) struct FullObject {
    pub(crate) id: ObjectId,
    pub(crate) created_at: EventId,
    pub(crate) creation: Arc<dyn Any + Send + Sync>,
    pub(crate) changes: Arc<BTreeMap<EventId, Change>>,
}

impl FullObject {
    pub(crate) async fn apply<T: Object>(
        &mut self,
        id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            id > self.created_at,
            "Submitted event {id:?} before the last recreation time ({:?}) of object {:?}",
            self.created_at,
            self.id,
        );
        if let Some(c) = self.changes.get(&id) {
            anyhow::ensure!(
                c.event
                    .clone()
                    .downcast::<T::Event>()
                    .map(|e| e == event)
                    .unwrap_or(false),
                "Event {id:?} was already pushed to object {:?} with a different value",
                self.id,
            );
        }

        // Get the snapshot to just before the new event
        let (_, mut last_snapshot) = self
            .get_snapshot_at::<T>(Bound::Excluded(id))
            .with_context(|| format!("applying event {id:?}"))?;

        // Apply the new event
        let last_snapshot_mut = Arc::make_mut(&mut last_snapshot);
        last_snapshot_mut.apply(&*event);
        let new_change = Change {
            event,
            snapshot_after: Some(last_snapshot),
        };
        let changes_mut = Arc::make_mut(&mut self.changes);
        anyhow::ensure!(
            changes_mut.insert(id, new_change).is_none(),
            "Object {:?} already had an event with id {id:?}",
            self.id,
        );

        // Finally, invalidate all snapshots since the event
        let to_invalidate = changes_mut.range_mut((Bound::Excluded(id), Bound::Unbounded));
        for c in to_invalidate {
            c.1.snapshot_after = None;
        }

        Ok(())
    }

    pub(crate) fn snapshot<T: Object>(&mut self, at: Timestamp) -> anyhow::Result<()> {
        let max_new_created_at = EventId(Ulid::from_parts(at.0 + 1, 0));
        let (new_created_at, snapshot) =
            self.get_snapshot_at::<T>(Bound::Excluded(max_new_created_at))?;
        self.created_at = new_created_at;
        self.creation = snapshot;
        let changes = Arc::make_mut(&mut self.changes);
        *changes = changes.split_off(&new_created_at);
        changes.pop_first();
        Ok(())
    }

    fn get_snapshot_at<T: Object>(
        &mut self,
        at: Bound<EventId>,
    ) -> anyhow::Result<(EventId, Arc<T>)> {
        // Find the last snapshot before `at`
        let changes_before = self.changes.range((Bound::Unbounded, at));
        let mut last_snapshot = None;
        for c in changes_before.rev() {
            if let Some(s) = c.1.snapshot_after.as_ref() {
                last_snapshot = Some((c.0, s.clone()));
                break;
            }
        }
        let (last_snapshot_time, last_snapshot) = match last_snapshot {
            Some((t, s)) => (*t, s),
            // Note: Casting ObjectId to EventId here because ordering is the same anyway
            None => (self.created_at, self.creation.clone()),
        };
        let mut last_snapshot = last_snapshot
            .downcast::<T>()
            .map_err(|_| anyhow!("Failed downcasting {:?} to type {:?}", self.id, T::ulid()))?;
        let last_snapshot_mut = Arc::make_mut(&mut last_snapshot);

        // Iterate through the changes since the last snapshot to just before the event
        let to_apply = self
            .changes
            .range((Bound::Excluded(last_snapshot_time), at));
        let mut last_event_time = last_snapshot_time;
        for (id, change) in to_apply {
            last_event_time = *id;
            last_snapshot_mut.apply(
                change
                    .event
                    .downcast_ref()
                    .expect("Event with different type than object type"),
            );
        }

        Ok((last_event_time, last_snapshot))
    }
}

pub(crate) struct NewObject {
    type_id: TypeId,
    id: ObjectId,
    value: Arc<dyn Any + Send + Sync>,
}

pub(crate) struct NewEvent {
    type_id: TypeId,
    object_id: ObjectId,
    id: EventId,
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

pub(crate) trait Db: 'static + Send + Sync {
    /// These streams get new elements whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn new_objects(&self) -> impl Send + Future<Output = impl Send + Stream<Item = NewObject>>;
    /// This function returns all new events for events on objects that have been
    /// subscribed on. Objects subscribed on are all the objects that have ever been
    /// created with `created`, or obtained with `get` or `query`, as well as all
    /// objects received through `new_objects`, excluding objects explicitly unsubscribed
    /// from
    fn new_events(&self) -> impl Send + Future<Output = impl Send + Stream<Item = NewEvent>>;
    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()>;

    async fn create<T: Object>(&self, object_id: ObjectId, object: Arc<T>) -> anyhow::Result<()>;
    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()>;

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    async fn query(
        &self,
        type_id: TypeId,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>>;

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()>;

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()>;
    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Arc<Vec<u8>>>;
}
