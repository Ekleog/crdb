#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::db_trait::{EventId, ObjectId};
use ulid::Ulid;

pub const OBJECT_ID_1: ObjectId = ObjectId(Ulid::from_bytes(*b"0000000000000001"));
pub const OBJECT_ID_2: ObjectId = ObjectId(Ulid::from_bytes(*b"0000000000000002"));
pub const OBJECT_ID_3: ObjectId = ObjectId(Ulid::from_bytes(*b"0000000000000003"));
pub const OBJECT_ID_4: ObjectId = ObjectId(Ulid::from_bytes(*b"0000000000000004"));
pub const OBJECT_ID_5: ObjectId = ObjectId(Ulid::from_bytes(*b"0000000000000005"));

pub const EVENT_ID_1: EventId = EventId(Ulid::from_bytes(*b"1000000000000001"));
pub const EVENT_ID_2: EventId = EventId(Ulid::from_bytes(*b"1000000000000002"));
pub const EVENT_ID_3: EventId = EventId(Ulid::from_bytes(*b"1000000000000003"));
pub const EVENT_ID_4: EventId = EventId(Ulid::from_bytes(*b"1000000000000004"));
pub const EVENT_ID_5: EventId = EventId(Ulid::from_bytes(*b"1000000000000005"));

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
pub struct TestObject1(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>);

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
pub enum TestEvent1 {
    Set(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Append(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Clear,
}

impl TestObject1 {
    pub fn stub_1() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_2() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_3() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_4() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_5() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
}

#[allow(unused_variables)] // TODO: remove?
impl crate::Object for TestObject1 {
    type Event = TestEvent1;

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
