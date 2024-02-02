use super::FullObject;
use crate::{
    test_utils::{
        TestEventSimple, TestObjectSimple, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4,
        OBJECT_ID_1,
    },
    Timestamp, Updatedness,
};
use std::sync::Arc;

#[test]
fn regression_apply_1324_does_not_type_error() {
    let o = FullObject::new(
        OBJECT_ID_1,
        None,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
    );
    o.apply::<TestObjectSimple>(
        EVENT_ID_3,
        Arc::new(TestEventSimple::Set(b"3".to_vec())),
        None,
    )
    .unwrap();
    o.apply::<TestObjectSimple>(
        EVENT_ID_2,
        Arc::new(TestEventSimple::Set(b"2".to_vec())),
        None,
    )
    .unwrap();
    o.apply::<TestObjectSimple>(
        EVENT_ID_4,
        Arc::new(TestEventSimple::Set(b"4".to_vec())),
        None,
    )
    .unwrap();
}
