use super::ObjectCache;
use crate::{test_utils::*, EventId, ObjectId, Timestamp};
use std::sync::Arc;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Create {
        id: ObjectId,
        created_at: EventId,
        object: TestObjectSimple,
    },
    Remove(usize),
    Submit {
        object: usize,
        event_id: EventId,
        event: TestEventSimple,
    },
    Recreate {
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

fn cache_state_stays_valid_impl((watermark, ops): &(usize, Vec<Op>)) {
    let mut cache = ObjectCache::new(*watermark);
    let mut objects = Vec::new();
    let mut locations = vec![None; 0x10000];
    for (i, op) in ops.iter().enumerate() {
        match op {
            Op::Create {
                id,
                created_at,
                object,
            } => {
                if objects.iter().all(|v| v != id) {
                    cache
                        .create(*id, *created_at, Arc::new(object.clone()))
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
                objects.get(*object).map(|id| {
                    cache.submit::<TestObjectSimple>(*id, *event_id, Arc::new(event.clone()))
                });
            }
            Op::Recreate { object, time } => {
                objects
                    .get(*object)
                    .map(|id| cache.recreate::<TestObjectSimple>(*id, *time));
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
        cache.assert_invariants(|| format!("after processing op {i}: {op:?}"));
    }
}

#[test]
fn cache_state_stays_valid() {
    bolero::check!()
        .with_type()
        .for_each(cache_state_stays_valid_impl);
}

#[test]
fn regression_submit_before_object_tracks_size_ok() {
    let mut cache = ObjectCache::new(1000);
    cache
        .create(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestObjectSimple::stub_1()),
        )
        .unwrap();
    // ignore submit result, as we'll be expecting a failure here
    let _ = cache.submit::<TestObjectSimple>(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestEventSimple::Set(b"12345678".to_vec())),
    );
    cache.assert_invariants(|| "regression test".to_string());
}

#[test]
fn regression_submit_after_object_tracks_ids_ok() {
    let mut cache = ObjectCache::new(1000);
    cache
        .create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
        )
        .unwrap();
    cache
        .submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestEventSimple::Set(b"12345678".to_vec())),
        )
        .unwrap();
    cache.assert_invariants(|| "regression test".to_string());
}

#[test]
fn regression_submit_order_1324_leads_to_type_corruption() {
    let mut cache = ObjectCache::new(1000);
    cache
        .create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
        )
        .unwrap();
    cache
        .submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_3,
            Arc::new(TestEventSimple::Append(b"5678".to_vec())),
        )
        .unwrap();
    cache
        .submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestEventSimple::Set(b"ABCD".to_vec())),
        )
        .unwrap();
    cache
        .submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_4,
            Arc::new(TestEventSimple::Set(b"EFGH".to_vec())),
        )
        .unwrap();
    cache.assert_invariants(|| "regression test".to_string());
}

#[test]
fn regression_double_recreate_panics() {
    let mut cache = ObjectCache::new(1000);
    cache
        .create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
        )
        .unwrap();
    cache
        .submit::<TestObjectSimple>(
            OBJECT_ID_1,
            EVENT_ID_2,
            Arc::new(TestEventSimple::Set(b"1234".to_vec())),
        )
        .unwrap();
    cache
        .recreate::<TestObjectSimple>(OBJECT_ID_1, Timestamp::from_ms(1000))
        .unwrap();
    cache
        .recreate::<TestObjectSimple>(OBJECT_ID_1, Timestamp::from_ms(2000))
        .unwrap();
    cache.assert_invariants(|| "regression test".to_string());
}

#[test]
fn regression_recerate_too_late_fails_size_check() {
    let mut cache = ObjectCache::new(1000);
    cache
        .create(
            OBJECT_ID_1,
            EVENT_ID_1,
            Arc::new(TestObjectSimple::stub_1()),
        )
        .unwrap();
    cache.assert_invariants(|| "regression test".to_string());
    // this will fail due to Timestamp being too big
    let _ = cache.recreate::<TestObjectSimple>(OBJECT_ID_1, Timestamp::from_ms(u64::MAX));
    cache.assert_invariants(|| "regression test".to_string());
}

#[test]
fn regression_remove_to_empty_makes_integer_overflow() {
    let mut cache = ObjectCache::new(0);
    let created = Arc::new(TestObjectSimple::stub_1()); // keep alive for watermark to not directly erase it
    cache
        .create(OBJECT_ID_1, EVENT_ID_1, created.clone())
        .unwrap();
    cache.remove(&OBJECT_ID_1);
    cache.assert_invariants(|| "regression test".to_string());
}
