use super::ulid;
use crate::{
    test_utils::USER_ID_NULL, BinPtr, CanDoCallbacks, DbPtr, Object, ObjectId, TypeId, User,
};

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
pub struct TestObjectPerms(pub User);

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
pub enum TestEventPerms {
    Set(User),
}

impl TestObjectPerms {
    pub fn standardize(&mut self, _self_id: ObjectId) {}
}

impl Object for TestObjectPerms {
    type Event = TestEventPerms;

    fn type_ulid() -> &'static TypeId {
        static TYPE: TypeId = TypeId(ulid("01HKKA237SXP3PWNE80DAX5PGH"));
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
        Ok(vec![USER_ID_NULL, self.0])
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            TestEventPerms::Set(u) => self.0 = *u,
        }
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl crate::Event for TestEventPerms {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}
