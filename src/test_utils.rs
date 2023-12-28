#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::db_trait::{EventId, ObjectId, TypeId};
use ulid::Ulid;

const fn ulid(s: &str) -> Ulid {
    match Ulid::from_string(s) {
        Ok(r) => r,
        Err(_) => panic!("const fn failed"),
    }
}

pub const OBJECT_ID_NULL: ObjectId = ObjectId(ulid("00000000000000000000000000"));
pub const OBJECT_ID_1: ObjectId = ObjectId(ulid("10000000000000000000000010"));
pub const OBJECT_ID_2: ObjectId = ObjectId(ulid("10000000000000000000000020"));
pub const OBJECT_ID_3: ObjectId = ObjectId(ulid("10000000000000000000000030"));
pub const OBJECT_ID_4: ObjectId = ObjectId(ulid("10000000000000000000000040"));
pub const OBJECT_ID_5: ObjectId = ObjectId(ulid("10000000000000000000000050"));

pub const EVENT_ID_NULL: EventId = EventId(ulid("00000000000000000000000000"));
pub const EVENT_ID_1: EventId = EventId(ulid("20000000000000000000000010"));
pub const EVENT_ID_2: EventId = EventId(ulid("20000000000000000000000020"));
pub const EVENT_ID_3: EventId = EventId(ulid("20000000000000000000000030"));
pub const EVENT_ID_4: EventId = EventId(ulid("20000000000000000000000040"));
pub const EVENT_ID_5: EventId = EventId(ulid("20000000000000000000000050"));

pub const TYPE_ID_NULL: TypeId = TypeId(ulid("00000000000000000000000000"));
pub const TYPE_ID_1: TypeId = TypeId(ulid("30000000000000000000000010"));
pub const TYPE_ID_2: TypeId = TypeId(ulid("30000000000000000000000020"));
pub const TYPE_ID_3: TypeId = TypeId(ulid("30000000000000000000000030"));
pub const TYPE_ID_4: TypeId = TypeId(ulid("30000000000000000000000040"));
pub const TYPE_ID_5: TypeId = TypeId(ulid("30000000000000000000000050"));

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
    pub fn new(v: Vec<u8>) -> TestObject1 {
        TestObject1(v)
    }
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
        match event {
            TestEvent1::Set(v) => self.0 = v.clone(),
            TestEvent1::Append(v) => self.0.extend(v.iter().cloned()),
            TestEvent1::Clear => self.0.clear(),
        }
    }

    fn is_heavy(&self) -> anyhow::Result<bool> {
        todo!()
    }

    fn required_binaries(&self) -> Vec<crate::BinPtr> {
        todo!()
    }
}
