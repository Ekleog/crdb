use crate::{ulid, USER_ID_NULL};
use crdb_core::{BinPtr, CanDoCallbacks, DbPtr, Event, Object, ObjectId, TypeId, User};
use std::collections::HashSet;

#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    arbitrary::Arbitrary,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct TestObjectSimple(pub Vec<u8>);

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    arbitrary::Arbitrary,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum TestEventSimple {
    Set(Vec<u8>),
    Append(Vec<u8>),
    Clear,
}

impl TestObjectSimple {
    pub fn new(v: Vec<u8>) -> TestObjectSimple {
        TestObjectSimple(v)
    }
    pub fn standardize(&mut self, _self_id: ObjectId) {}
    pub fn stub_1() -> TestObjectSimple {
        TestObjectSimple(b"10000001".to_vec())
    }
    pub fn stub_2() -> TestObjectSimple {
        TestObjectSimple(b"10000001".to_vec())
    }
    pub fn stub_3() -> TestObjectSimple {
        TestObjectSimple(b"10000001".to_vec())
    }
    pub fn stub_4() -> TestObjectSimple {
        TestObjectSimple(b"10000001".to_vec())
    }
    pub fn stub_5() -> TestObjectSimple {
        TestObjectSimple(b"10000001".to_vec())
    }
}

impl Object for TestObjectSimple {
    type Event = TestEventSimple;

    fn type_ulid() -> &'static TypeId {
        static TYPE: TypeId = TypeId(ulid("01HKK9WR2HJ8XW3Z8ZD4BGEZV5"));
        &TYPE
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _self_id: ObjectId,
        _db: &'a C,
    ) -> crate::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _self_id: ObjectId,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> crate::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
    ) -> crate::Result<HashSet<User>> {
        Ok([USER_ID_NULL].into_iter().collect())
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            TestEventSimple::Set(v) => self.0 = v.clone(),
            TestEventSimple::Append(v) => self.0.extend(v.iter().cloned()),
            TestEventSimple::Clear => self.0.clear(),
        }
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl Event for TestEventSimple {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}
