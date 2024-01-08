use super::ulid;
use crate::{BinPtr, CanDoCallbacks, DbPtr, Event, Object, ObjectId, TypeId, User};

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
pub struct TestObjectSimple(
    #[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] pub Vec<u8>,
);

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
pub enum TestEventSimple {
    Set(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Append(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Clear,
}

impl TestObjectSimple {
    pub fn new(v: Vec<u8>) -> TestObjectSimple {
        TestObjectSimple(v)
    }
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
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _self_id: ObjectId,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        Ok(Vec::new())
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            TestEventSimple::Set(v) => self.0 = v.clone(),
            TestEventSimple::Append(v) => self.0.extend(v.iter().cloned()),
            TestEventSimple::Clear => self.0.clear(),
        }
    }

    fn is_heavy(&self) -> bool {
        self.0.len() > 10
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
