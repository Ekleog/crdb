#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::{full_object::DynSized, BinPtr, CanDoCallbacks, DbPtr, Object, TypeId, User};
use anyhow::Context;
use std::any::Any;

mod mem_db;
mod stubs;

pub use mem_db::MemDb;
pub use stubs::*;

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
        &TYPE_ID_1
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
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

    fn apply(&mut self, event: &Self::Event) {
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

impl crate::Event for TestEventSimple {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

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
pub struct TestObjectPerms(pub User);

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
pub enum TestEventPerms {
    Set(User),
}

impl Object for TestObjectPerms {
    type Event = TestEventPerms;

    fn type_ulid() -> &'static TypeId {
        &TYPE_ID_2
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        Ok(vec![self.0])
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            TestEventPerms::Set(u) => self.0 = *u,
        }
    }

    fn is_heavy(&self) -> bool {
        false
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
pub struct TestObjectDelegatePerms(pub DbPtr<TestObjectPerms>);

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
pub enum TestEventDelegatePerms {
    Set(DbPtr<TestObjectPerms>),
}

impl Object for TestObjectDelegatePerms {
    type Event = TestEventDelegatePerms;

    fn type_ulid() -> &'static TypeId {
        &TYPE_ID_3
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        let remote = match db.get(self.0).await {
            Ok(r) => r,
            Err(crate::Error::Other(e)) => panic!("got unexpected error {e:?}"),
            _ => return Ok(Vec::new()), // protocol not respected
        };
        Ok(vec![remote.0])
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            TestEventDelegatePerms::Set(p) => self.0 = *p,
        }
    }

    fn is_heavy(&self) -> bool {
        false
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

crate::db! {
    pub mod db {
        auth: (),
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: ClientDb,
        objects: {
            test1: super::TestObjectSimple,
            perms: super::TestObjectPerms,
            delegate_perms: super::TestObjectDelegatePerms,
        },
    }
}

fn eq<T: 'static + Any + Send + Sync + Eq>(
    l: &dyn DynSized,
    r: &dyn DynSized,
) -> anyhow::Result<bool> {
    Ok(l.ref_to_any()
        .downcast_ref::<T>()
        .context("downcasting lhs")?
        == r.ref_to_any()
            .downcast_ref::<T>()
            .context("downcasting rhs")?)
}
