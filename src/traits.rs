use crate::{
    api::{BinPtr, Query},
    Object,
};
use anyhow::{anyhow, Context};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, ops::Bound, sync::Arc};
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
            id.0 > self.id.0,
            "Submitted event {id:?} before object's creation time {:?}",
            self.id,
        );

        // Get the snapshot to just before the event
        let mut last_snapshot = self
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

    fn get_snapshot_at<T: Object>(&mut self, at: Bound<EventId>) -> anyhow::Result<Arc<T>> {
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
            None => (EventId(self.id.0), self.creation.clone()),
        };
        let mut last_snapshot = last_snapshot
            .downcast::<T>()
            .map_err(|_| anyhow!("Failed downcasting {:?} to type {:?}", self.id, T::ulid()))?;
        let last_snapshot_mut = Arc::make_mut(&mut last_snapshot);

        // Iterate through the changes since the last snapshot to just before the event
        let to_apply = self
            .changes
            .range((Bound::Excluded(last_snapshot_time), at));
        for c in to_apply {
            last_snapshot_mut.apply(
                c.1.event
                    .downcast_ref()
                    .expect("Event with different type than object type"),
            );
        }

        Ok(last_snapshot)
    }
}

pub(crate) struct NewObject {
    time: Timestamp,
    type_id: TypeId,
    id: ObjectId,
    value: Arc<dyn Any + Send + Sync>,
}

pub(crate) struct NewEvent {
    time: Timestamp,
    type_id: TypeId,
    object_id: ObjectId,
    id: EventId,
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

pub(crate) trait Db {
    /// These functions are called whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn set_new_object_cb(&mut self, cb: Box<dyn Fn(NewObject)>);
    fn set_new_event_cb(&mut self, cb: Box<dyn Fn(NewEvent)>);

    async fn create<T: Object>(&self, object_id: ObjectId, object: Arc<T>) -> anyhow::Result<()>;
    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()>;

    async fn get(&self, ptr: ObjectId) -> anyhow::Result<FullObject>;
    async fn query(
        &self,
        type_id: TypeId,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = FullObject>>;

    async fn snapshot(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()>;

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()>;
    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Arc<Vec<u8>>>;
}
