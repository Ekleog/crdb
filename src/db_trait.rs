use crate::{
    api::{BinPtr, Query},
    Object, User,
};
use anyhow::{anyhow, Context};
use futures::Stream;
use std::{any::Any, collections::BTreeMap, future::Future, ops::Bound, sync::Arc};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectId(pub Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId(pub Ulid);
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TypeId(pub Ulid);

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
    pub(crate) event: Arc<dyn Any + Send + Sync>,
    snapshot_after: Option<Arc<dyn Any + Send + Sync>>,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct FullObject {
    pub(crate) id: ObjectId,
    pub(crate) created_at: EventId,
    pub(crate) creation: Arc<dyn Any + Send + Sync>,
    pub(crate) changes: Arc<BTreeMap<EventId, Change>>,
}

impl FullObject {
    /// Returns `true` if the event was newly applied. Returns `false` if the same event had
    /// already been applied. Returns an error if another event with the same id had already
    /// been applied, if the event is earlier than the object's last recreation time, or if
    /// the provided `T` is wrong.
    pub(crate) fn apply<T: Object>(
        &mut self,
        id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<bool> {
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
            return Ok(false);
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
        assert!(
            changes_mut.insert(id, new_change).is_none(),
            "Object {:?} already had an event with id {id:?} despite earlier check",
            self.id,
        );

        // Finally, invalidate all snapshots since the event
        let to_invalidate = changes_mut.range_mut((Bound::Excluded(id), Bound::Unbounded));
        for c in to_invalidate {
            c.1.snapshot_after = None;
        }

        Ok(true)
    }

    pub(crate) fn recreate_at<T: Object>(&mut self, at: Timestamp) -> anyhow::Result<()> {
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

    pub fn last_snapshot<T: Object>(&mut self) -> anyhow::Result<Arc<T>> {
        // TODO: This currently does CoW to build the last snapshot, and thus does
        // not update the cache. The whole goal of the in-memory cache was to avoid
        // having to recompute the snapshots all the time. Somehow fix this (eg. use
        // concread data structures?)
        Ok(self.get_snapshot_at(Bound::Unbounded)?.1)
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

#[doc(hidden)]
#[derive(Clone)]
pub struct NewObject {
    pub type_id: TypeId,
    pub id: ObjectId,
    pub created_at: EventId,
    pub object: Arc<dyn Any + Send + Sync>,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct NewEvent {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub id: EventId,
    pub event: Arc<dyn Any + Send + Sync>,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct NewSnapshot {
    pub type_id: TypeId,
    pub object_id: ObjectId,
    pub time: Timestamp,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(u64); // Milliseconds since UNIX_EPOCH

#[doc(hidden)]
pub trait Db: 'static + Send + Sync {
    /// These streams get new elements whenever another user submitted a new object or event.
    /// Note that they are NOT called when you yourself called create or submit.
    fn new_objects(&self) -> impl Send + Future<Output = impl Send + Stream<Item = NewObject>>;
    /// This function returns all new events for events on objects that have been
    /// subscribed on. Objects subscribed on are all the objects that have ever been
    /// created with `created`, or obtained with `get` or `query`, as well as all
    /// objects received through `new_objects`, excluding objects explicitly unsubscribed
    /// from
    fn new_events(&self) -> impl Send + Future<Output = impl Send + Stream<Item = NewEvent>>;
    fn new_snapshots(&self) -> impl Send + Future<Output = impl Send + Stream<Item = NewSnapshot>>;
    /// Note that this function unsubscribes ALL the streams that have ever been taken from this
    /// database; and purges it from the local database.
    fn unsubscribe(&self, ptr: ObjectId) -> impl Send + Future<Output = anyhow::Result<()>>;

    fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn create_all<T: Object>(
        &self,
        o: FullObject,
    ) -> impl Send + Future<Output = anyhow::Result<()>> {
        async move {
            self.create::<T>(
                o.id,
                o.created_at,
                o.creation
                    .clone()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("API returned object of unexpected type"))?,
            )
            .await?;
            for (event_id, c) in o.changes.iter() {
                self.submit::<T>(
                    o.id,
                    *event_id,
                    c.event
                        .clone()
                        .downcast::<T::Event>()
                        .map_err(|_| anyhow!("API returned object of unexpected type"))?,
                )
                .await?;
            }
            Ok(())
        }
    }

    fn get<T: Object>(
        &self,
        ptr: ObjectId,
    ) -> impl Send + Future<Output = anyhow::Result<Option<FullObject>>>;
    /// Note: this function can also be used to populate the cache, as the cache will include
    /// any item returned by this function.
    fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> impl Send + Future<Output = anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>>>;

    fn snapshot<T: Object>(
        &self,
        time: Timestamp,
        object: ObjectId,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

    fn create_binary(
        &self,
        id: BinPtr,
        value: Arc<Vec<u8>>,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
    fn get_binary(
        &self,
        ptr: BinPtr,
    ) -> impl Send + Future<Output = anyhow::Result<Option<Arc<Vec<u8>>>>>;
}
