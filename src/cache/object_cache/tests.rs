use super::ObjectCache;
use crate::{
    db_trait::{EventId, ObjectId},
    Timestamp,
};
use std::sync::Arc;

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    bolero::generator::TypeGenerator,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
struct Object(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>);

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    bolero::generator::TypeGenerator,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
enum Event {
    Set(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Append(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Clear,
}

#[allow(unused_variables)] // TODO: remove?
impl crate::Object for Object {
    type Event = Event;

    fn ulid() -> &'static ulid::Ulid {
        todo!()
    }

    fn can_create<C: crate::CanDoCallbacks>(
        &self,
        user: crate::User,
        db: &C,
    ) -> anyhow::Result<bool> {
        todo!()
    }

    fn can_apply<C: crate::CanDoCallbacks>(
        &self,
        user: &crate::User,
        event: &Self::Event,
        db: &C,
    ) -> anyhow::Result<bool> {
        todo!()
    }

    fn users_who_can_read<C: crate::CanDoCallbacks>(&self) -> anyhow::Result<Vec<crate::User>> {
        todo!()
    }

    fn apply(&mut self, event: &Self::Event) {
        todo!()
    }

    fn is_heavy(&self) -> anyhow::Result<bool> {
        todo!()
    }

    fn required_binaries(&self) -> Vec<crate::BinPtr> {
        todo!()
    }
}

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Create {
        id: ObjectId,
        created_at: EventId,
        object: Arc<Object>,
    },
    Remove(usize),
    Submit {
        object: usize,
        event_id: EventId,
        event: Arc<Event>,
    },
    Snapshot {
        object: usize,
        time: Timestamp,
    },
    Get {
        object: usize,
        location: u16,
    },
    Release {
        location: u16,
    },
    Clear,
    ReduceSizeTo(usize),
    ReduceSize {
        max_items_checked: usize,
        max_size_removed: usize,
    },
}

// TODO: check for equivalence between `.insert()` and `.create()`/`.submit()`

#[test]
fn cache_state_stays_valid() {
    bolero::check!()
        .with_type()
        .for_each(|(watermark, ops): &(usize, Vec<Op>)| {
            let mut cache = ObjectCache::new(*watermark);
            let mut objects = Vec::new();
            let mut locations = vec![None; 0x10000];
            for op in ops {
                match op {
                    Op::Create {
                        id,
                        created_at,
                        object,
                    } => {
                        if objects.iter().all(|v| v != id) {
                            cache
                                .create(*id, *created_at, object.clone())
                                .expect("failed creating object");
                            objects.push(*id);
                        }
                    }
                    Op::Remove(o) => {
                        objects.get(*o).map(|id| cache.remove(id));
                    }
                    Op::Submit {
                        object,
                        event_id,
                        event,
                    } => {
                        objects
                            .get(*object)
                            .map(|id| cache.submit::<Object>(*id, *event_id, event.clone()));
                    }
                    Op::Snapshot { object, time } => {
                        objects
                            .get(*object)
                            .map(|id| cache.snapshot::<Object>(*id, *time));
                    }
                    Op::Get { object, location } => {
                        objects
                            .get(*object)
                            .map(|id| locations[*location as usize] = cache.get(id).cloned());
                    }
                    Op::Release { location } => {
                        locations[*location as usize].take();
                    }
                    Op::Clear => {
                        cache.clear();
                    }
                    Op::ReduceSizeTo(s) => {
                        cache.reduce_size_to(*s);
                    }
                    Op::ReduceSize {
                        max_items_checked,
                        max_size_removed,
                    } => {
                        cache.reduce_size(*max_items_checked, *max_size_removed);
                    }
                }
                cache.assert_invariants();
            }
        });
}
