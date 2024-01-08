#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::full_object::DynSized;
use anyhow::Context;
use std::any::Any;

mod mem_db;
mod object_delegate_perms;
mod object_full;
mod object_perms;
mod object_simple;
mod stubs;

pub use mem_db::MemDb;
pub use object_delegate_perms::{TestEventDelegatePerms, TestObjectDelegatePerms};
#[allow(unused_imports)] // TODO: add fuzzer-full
pub use object_full::{TestEventFull, TestObjectFull};
pub use object_perms::{TestEventPerms, TestObjectPerms};
pub use object_simple::{TestEventSimple, TestObjectSimple};
pub use stubs::*;

crate::db! {
    pub mod db {
        auth: (),
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: ClientDb,
        objects: {
            delegate_perms: super::TestObjectDelegatePerms,
            full: super::TestObjectFull,
            perms: super::TestObjectPerms,
            simple: super::TestObjectSimple,
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
