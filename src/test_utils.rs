#![allow(dead_code, unused_imports)] // test utils can be or not be used but get copy-pasted anyway

use crate::{CrdbStream, DynSized, Object};
use anyhow::Context;
use futures::StreamExt;
use std::{any::Any, fmt::Debug, sync::Arc};

mod full_object;
mod mem_db;
mod object_delegate_perms;
mod object_full;
mod object_perms;
mod object_simple;
mod smoke_test;
mod stubs;

pub use full_object::FullObject;
pub use mem_db::MemDb;
pub use object_delegate_perms::{TestEventDelegatePerms, TestObjectDelegatePerms};
pub use object_full::{TestEventFull, TestObjectFull};
pub use object_perms::{TestEventPerms, TestObjectPerms};
pub use object_simple::{TestEventSimple, TestObjectSimple};
pub use stubs::*;

crate::db! {
    pub mod db {
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

#[cfg(feature = "_tests")]
pub fn cmp_err(pg: &crate::Error, mem: &crate::Error) -> bool {
    use crate::Error::*;
    match (pg, mem) {
        (MissingBinaries(a), MissingBinaries(b)) => a == b,
        (InvalidTimestamp(a), InvalidTimestamp(b)) => a == b,
        (ObjectAlreadyExists(a), ObjectAlreadyExists(b)) => a == b,
        (EventAlreadyExists(a), EventAlreadyExists(b)) => a == b,
        (ObjectDoesNotExist(a), ObjectDoesNotExist(b)) => a == b,
        (TypeDoesNotExist(a), TypeDoesNotExist(b)) => a == b,
        (BinaryHashMismatch(a), BinaryHashMismatch(b)) => a == b,
        (NullByteInString, NullByteInString) => true,
        (InvalidNumber, InvalidNumber) => true,
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
        ) => event_id_1 == event_id_2 && object_id_1 == object_id_2 && created_at_1 == created_at_2,
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
    }
}

pub(crate) fn cmp_just_errs<T, U>(
    testdb_res: &crate::Result<T>,
    mem_res: &crate::Result<U>,
) -> anyhow::Result<()> {
    match (&testdb_res, &mem_res) {
        (Ok(_), Ok(_)) => (),
        (Err(testdb_err), Err(mem_err)) =>
            anyhow::ensure!(cmp_err(testdb_err, mem_err), "tested db err != mem err:\n==========\nTested DB:\n{testdb_err:?}\n==========\nMem:\n{mem_err:?}\n=========="),
        (Ok(_), Err(mem_err)) => anyhow::bail!("tested db is ok but mem had an error:\n==========\nMem:\n{mem_err:?}\n=========="),
        (Err(testdb_err), Ok(_)) => anyhow::bail!("mem is ok but tested db had an error:\n==========\nTested DB:\n{testdb_err:?}\n=========="),
    }
    Ok(())
}

#[cfg(feature = "_tests")]
pub fn cmp<T: Debug + Eq>(
    testdb_res: crate::Result<T>,
    mem_res: crate::Result<T>,
) -> anyhow::Result<()> {
    let is_eq = match (&testdb_res, &mem_res) {
        (_, Err(crate::Error::Other(mem))) => panic!("MemDb hit an internal server error: {mem:?}"),
        (Ok(testdb), Ok(mem)) => testdb == mem,
        (Err(testdb_err), Err(mem_err)) => cmp_err(testdb_err, mem_err),
        _ => false,
    };
    anyhow::ensure!(is_eq, "tested db result != mem result:\n==========\nTested DB:\n{testdb_res:?}\n==========\nMem:\n{mem_res:?}\n==========");
    Ok(())
}

#[macro_export] // used by the client-js.rs integration test
macro_rules! generic_op {
    ($name:ident) => {
        #[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
        enum $name {
            Remove { object_id: usize },
            Unlock { object_id: usize },
            Vacuum { recreate_at: Option<Timestamp> },
        }

        impl $name {
            async fn apply(&self, db: &Database, s: &mut FuzzState) -> anyhow::Result<()> {
                match self {
                    Self::Remove { object_id } => {
                        if !s.is_server {
                            let object_id = s.object(*object_id);
                            let db = db.remove(object_id).await;
                            let mem = s.mem_db.remove(object_id).await;
                            cmp(db, mem)?;
                        }
                    }
                    Self::Unlock { object_id } => {
                        if !s.is_server {
                            let object_id = s.object(*object_id);
                            let db = db.unlock(object_id).await;
                            let mem = s.mem_db.unlock(object_id).await;
                            cmp(db, mem)?;
                        }
                    }
                    Self::Vacuum { recreate_at } => {
                        run_vacuum(&db, &s.mem_db, *recreate_at).await?;
                    }
                }
                Ok(())
            }
        }
    };
}
