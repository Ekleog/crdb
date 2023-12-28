use crate::{
    db_trait::{EventId, ObjectId},
    Object, Timestamp,
};
use anyhow::{anyhow, Context};
use std::{
    any::Any,
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    sync::{Arc, RwLock},
};
use ulid::Ulid;

#[cfg(test)]
mod tests;

pub trait DynSized: 'static + Any + Send + Sync + deepsize::DeepSizeOf {
    // TODO: remove these functions once rust supports trait upcasting:
    // https://github.com/rust-lang/rust/issues/65991#issuecomment-1869869919
    // https://github.com/rust-lang/rust/issues/119335
    fn arc_to_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn ref_to_any(&self) -> &(dyn Any + Send + Sync);
    fn deep_size_of(&self) -> usize {
        <Self as deepsize::DeepSizeOf>::deep_size_of(self)
    }
}
impl<T: 'static + Any + Send + Sync + deepsize::DeepSizeOf> DynSized for T {
    fn arc_to_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
    fn ref_to_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }
}

fn fmt_option_arc(
    v: &Option<Arc<dyn DynSized>>,
    fmt: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match v {
        Some(v) => write!(fmt, "Some({:p})", &**v),
        None => write!(fmt, "None"),
    }
}

#[derive(Clone, deepsize::DeepSizeOf, educe::Educe)]
#[educe(Debug)]
pub struct Change {
    #[educe(Debug(method = std::fmt::Pointer::fmt))]
    pub event: Arc<dyn DynSized>,
    #[educe(Debug(method = fmt_option_arc))]
    pub snapshot_after: Option<Arc<dyn DynSized>>,
}

#[derive(deepsize::DeepSizeOf, educe::Educe)]
#[educe(Debug)]
struct FullObjectImpl {
    id: ObjectId,
    created_at: EventId,
    #[educe(Debug(method = std::fmt::Pointer::fmt))]
    creation: Arc<dyn DynSized>,
    changes: BTreeMap<EventId, Change>,
}

pub struct CreationInfo {
    pub id: ObjectId,
    pub created_at: EventId,
    pub creation: Arc<dyn DynSized>,
}

#[derive(Clone, Debug)]
pub struct FullObject {
    data: Arc<RwLock<FullObjectImpl>>,
}

impl FullObject {
    pub fn new(id: ObjectId, created_at: EventId, creation: Arc<dyn DynSized>) -> FullObject {
        FullObject {
            data: Arc::new(RwLock::new(FullObjectImpl {
                id,
                created_at,
                creation,
                changes: BTreeMap::new(),
            })),
        }
    }

    pub fn refcount(&self) -> usize {
        let mut res = Arc::strong_count(&self.data);
        let this = self.data.read().unwrap();
        res += Arc::strong_count(&this.creation) - 1;
        res += this
            .changes
            .values()
            .map(|v| {
                v.snapshot_after
                    .as_ref()
                    .map(|s| Arc::strong_count(&s) - 1)
                    .unwrap_or(0)
            })
            .sum::<usize>();
        res
    }

    pub fn id(&self) -> ObjectId {
        self.data.read().unwrap().id
    }

    pub fn creation_info(&self) -> CreationInfo {
        let this = self.data.read().unwrap();
        CreationInfo {
            id: this.id,
            created_at: this.created_at,
            creation: this.creation.clone(),
        }
    }

    pub fn changes_clone(&self) -> BTreeMap<EventId, Change> {
        self.data.read().unwrap().changes.clone()
    }

    pub fn extract_all_clone(&self) -> (CreationInfo, BTreeMap<EventId, Change>) {
        let this = self.data.read().unwrap();
        (
            CreationInfo {
                id: this.id,
                created_at: this.created_at,
                creation: this.creation.clone(),
            },
            this.changes.clone(),
        )
    }

    pub fn deep_size_of(&self) -> usize {
        self.data.read().unwrap().deep_size_of()
    }

    pub fn apply<T: Object>(&self, id: EventId, event: Arc<T::Event>) -> anyhow::Result<bool> {
        self.data.write().unwrap().apply::<T>(id, event)
    }

    pub fn recreate_at<T: Object>(&self, at: Timestamp) -> anyhow::Result<()> {
        self.data.write().unwrap().recreate_at::<T>(at)
    }

    pub fn last_snapshot<T: Object>(&self) -> anyhow::Result<Arc<T>> {
        {
            let this = self.data.read().unwrap();
            if this.changes.is_empty() {
                return Ok(this
                    .creation
                    .clone()
                    .arc_to_any()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("Downcasting already-typed element"))?);
            }
            let (_, last_change) = this.changes.last_key_value().unwrap();
            if let Some(s) = &last_change.snapshot_after {
                return Ok(s
                    .clone()
                    .arc_to_any()
                    .downcast::<T>()
                    .map_err(|_| anyhow!("Downcasting already-typed element"))?);
            }
        }
        Ok(self
            .data
            .write()
            .unwrap()
            .get_snapshot_at(Bound::Unbounded)?
            .1)
    }
}

impl FullObjectImpl {
    /// Returns `true` if the event was newly applied. Returns `false` if the same event had
    /// already been applied. Returns an error if another event with the same id had already
    /// been applied, if the event is earlier than the object's last recreation time, or if
    /// the provided `T` is wrong.
    pub fn apply<T: Object>(&mut self, id: EventId, event: Arc<T::Event>) -> anyhow::Result<bool> {
        anyhow::ensure!(
            id > self.created_at,
            "Submitted event {id:?} before the last recreation time ({:?}) of object {:?}",
            self.created_at,
            self.id,
        );
        if let Some(c) = self.changes.get(&id) {
            anyhow::ensure!(
                (*c.event)
                    .ref_to_any()
                    .downcast_ref::<T::Event>()
                    .map(|e| e == &*event)
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
        let last_snapshot_mut: &mut T = Arc::make_mut(&mut last_snapshot);
        last_snapshot_mut.apply(&*event);
        let new_change = Change {
            event,
            snapshot_after: Some(last_snapshot),
        };
        assert!(
            self.changes.insert(id, new_change).is_none(),
            "Object {:?} already had an event with id {id:?} despite earlier check",
            self.id,
        );

        // Finally, invalidate all snapshots since the event
        let to_invalidate = self
            .changes
            .range_mut((Bound::Excluded(id), Bound::Unbounded));
        for c in to_invalidate {
            c.1.snapshot_after = None;
        }

        Ok(true)
    }

    pub fn recreate_at<T: Object>(&mut self, at: Timestamp) -> anyhow::Result<()> {
        anyhow::ensure!(
            at.time_ms() < (1 << Ulid::TIME_BITS),
            "Recreating object at timestamp {at:?}, which is invalid for ULIDs"
        );
        let max_new_created_at =
            EventId(Ulid::from_parts(at.time_ms(), (1 << Ulid::RAND_BITS) - 1));

        // First, check that we're not trying to roll the snapshot back in time, as this would result
        // in passing invalid input to `get_snapshot_at`.
        if max_new_created_at <= self.created_at {
            return Ok(());
        }

        let (new_created_at, snapshot) =
            self.get_snapshot_at::<T>(Bound::Excluded(max_new_created_at))?;
        self.created_at = new_created_at;
        self.creation = snapshot;
        self.changes = self.changes.split_off(&new_created_at);
        self.changes.pop_first();
        Ok(())
    }

    /// Returns `(was_actually_last_in_bound, id, event)`
    fn last_snapshot_before(&self, at: Bound<EventId>) -> (bool, EventId, Arc<dyn DynSized>) {
        let changes_before = self.changes.range((Bound::Unbounded, at));
        let mut is_first = true;
        for (id, c) in changes_before.rev() {
            if let Some(s) = c.snapshot_after.as_ref() {
                return (is_first, *id, s.clone());
            }
            is_first = false;
        }
        (is_first, self.created_at, self.creation.clone())
    }

    fn get_snapshot_at<T: Object>(
        &mut self,
        at: Bound<EventId>,
    ) -> anyhow::Result<(EventId, Arc<T>)> {
        debug_assert!(
            (Bound::Unbounded, at).contains(&self.created_at),
            "asked `get_snapshot_at` for a too-early bound"
        );
        // Find the last snapshot before `at`
        let (_, last_snapshot_time, last_snapshot) = self.last_snapshot_before(at);
        let mut last_snapshot = last_snapshot
            .arc_to_any()
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
                (*change.event)
                    .ref_to_any()
                    .downcast_ref()
                    .expect("Event with different type than object type"),
            );
        }

        // Save the computed snapshot
        if last_event_time != last_snapshot_time {
            assert!(
                self.changes
                    .get_mut(&last_event_time)
                    .unwrap()
                    .snapshot_after
                    .replace(last_snapshot.clone())
                    .is_none(),
                "Recomputed snapshot that was already computed"
            );
        }

        Ok((last_event_time, last_snapshot))
    }
}
