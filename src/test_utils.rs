#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::full_object::DynSized;
use anyhow::Context;
use std::{any::Any, fmt::Debug};

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

pub(crate) fn cmp<T: Debug + Eq>(
    pg_res: crate::Result<T>,
    mem_res: crate::Result<T>,
) -> anyhow::Result<()> {
    use crate::Error::*;
    let is_eq = match (&pg_res, &mem_res) {
        (_, Err(Other(mem))) => panic!("MemDb hit an internal server error: {mem:?}"),
        (Ok(pg), Ok(mem)) => pg == mem,
        (Err(pg_err), Err(mem_err)) => match (pg_err, mem_err) {
            (MissingBinaries(a), MissingBinaries(b)) => a == b,
            (InvalidTimestamp(a), InvalidTimestamp(b)) => a == b,
            (ObjectAlreadyExists(a), ObjectAlreadyExists(b)) => a == b,
            (EventAlreadyExists(a), EventAlreadyExists(b)) => a == b,
            (ObjectDoesNotExist(a), ObjectDoesNotExist(b)) => a == b,
            (TypeDoesNotExist(a), TypeDoesNotExist(b)) => a == b,
            (BinaryHashMismatch(a), BinaryHashMismatch(b)) => a == b,
            (NullByteInString, NullByteInString) => true,
            (InvalidToken(a), InvalidToken(b)) => a == b,
            (
                EventTooEarly {
                    event_id: event_id_1,
                    object_id: object_id_1,
                    created_at: created_at_1,
                },
                EventTooEarly {
                    event_id: event_id_2,
                    object_id: object_id_2,
                    created_at: created_at_2,
                },
            ) => {
                event_id_1 == event_id_2
                    && object_id_1 == object_id_2
                    && created_at_1 == created_at_2
            }
            (
                WrongType {
                    object_id: object_id_1,
                    expected_type_id: expected_type_id_1,
                    real_type_id: real_type_id_1,
                },
                WrongType {
                    object_id: object_id_2,
                    expected_type_id: expected_type_id_2,
                    real_type_id: real_type_id_2,
                },
            ) => {
                object_id_1 == object_id_2
                    && expected_type_id_1 == expected_type_id_2
                    && real_type_id_1 == real_type_id_2
            }
            _ => false,
        },
        _ => false,
    };
    anyhow::ensure!(is_eq, "postgres result != mem result:\n==========\nPostgres:\n{pg_res:?}\n==========\nMem:\n{mem_res:?}");
    Ok(())
}
