use super::{ulid, TestObjectPerms};
use crate::{
    test_utils::USER_ID_NULL, BinPtr, CanDoCallbacks, DbPtr, Object, ObjectId, TypeId, User,
};
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
pub struct TestObjectDelegatePerms(pub DbPtr<TestObjectPerms>);

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
pub enum TestEventDelegatePerms {
    Set(DbPtr<TestObjectPerms>),
}

impl TestObjectDelegatePerms {
    pub fn standardize(&mut self, _self_id: ObjectId) {}
}

impl Object for TestObjectDelegatePerms {
    type Event = TestEventDelegatePerms;

    fn type_ulid() -> &'static TypeId {
        static TYPE: TypeId = TypeId(ulid("01HKKA5R16P2WXYG4SZVG2G1R6"));
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
        db: &'a C,
    ) -> anyhow::Result<HashSet<User>> {
        let remote = match db.get(self.0).await {
            Ok(r) => r,
            Err(crate::Error::Other(e)) => panic!("got unexpected error {e:?}"),
            _ => return Ok(HashSet::new()), // protocol not respected
        };
        Ok([USER_ID_NULL, remote.0].into_iter().collect())
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            TestEventDelegatePerms::Set(p) => self.0 = *p,
        }
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl crate::Event for TestEventDelegatePerms {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}
