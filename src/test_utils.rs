#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::{full_object::DynSized, BinPtr, CanDoCallbacks, DbPtr, Object, TypeId, User};
use anyhow::Context;
use std::any::Any;

mod mem_db;
mod object_perms;
mod object_simple;
mod stubs;

pub use mem_db::MemDb;
pub use object_perms::{TestEventPerms, TestObjectPerms};
pub use object_simple::{TestEventSimple, TestObjectSimple};
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
