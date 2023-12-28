use super::FullObject;
use crate::test_utils::{
    TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4, OBJECT_ID_1,
};
use std::sync::Arc;

// TODO: properly fuzz the whole FullObject API, checking invariants like snapshot validity

#[test]
fn regression_apply_1324_does_not_type_error() {
    let o = FullObject::new(OBJECT_ID_1, EVENT_ID_1, Arc::new(TestObject1::stub_1()));
    o.apply::<TestObject1>(EVENT_ID_3, Arc::new(TestEvent1::Set(b"3".to_vec())))
        .unwrap();
    o.apply::<TestObject1>(EVENT_ID_2, Arc::new(TestEvent1::Set(b"2".to_vec())))
        .unwrap();
    o.apply::<TestObject1>(EVENT_ID_4, Arc::new(TestEvent1::Set(b"4".to_vec())))
        .unwrap();
}
