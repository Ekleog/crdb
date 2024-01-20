#![allow(dead_code, unused_imports)] // test utils can be or not be used but get copy-pasted anyway

use crate::{
    full_object::{DynSized, FullObject},
    CrdbStream, Object,
};
use anyhow::Context;
use futures::StreamExt;
use std::{any::Any, fmt::Debug, sync::Arc};

mod mem_db;
mod object_delegate_perms;
mod object_full;
mod object_perms;
mod object_simple;
mod smoke_test;
mod stubs;

pub use mem_db::MemDb;
pub use object_delegate_perms::{TestEventDelegatePerms, TestObjectDelegatePerms};
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
    pg_res: &crate::Result<T>,
    mem_res: &crate::Result<U>,
) -> anyhow::Result<()> {
    match (&pg_res, &mem_res) {
        (Ok(_), Ok(_)) => (),
        (Err(pg_err), Err(mem_err)) =>
            anyhow::ensure!(cmp_err(pg_err, mem_err), "postgres err != mem err:\n==========\nPostgres:\n{pg_err:?}\n==========\nMem:\n{mem_err:?}\n=========="),
        (Ok(_), Err(mem_err)) => anyhow::bail!("pg is ok but mem had an error:\n==========\nMem:\n{mem_err:?}\n=========="),
        (Err(pg_err), Ok(_)) => anyhow::bail!("mem is ok but pg had an error:\n==========\nPostgres:\n{pg_err:?}\n=========="),
    }
    Ok(())
}

#[cfg(feature = "_tests")]
pub fn cmp<T: Debug + Eq>(
    pg_res: crate::Result<T>,
    mem_res: crate::Result<T>,
) -> anyhow::Result<()> {
    let is_eq = match (&pg_res, &mem_res) {
        (_, Err(crate::Error::Other(mem))) => panic!("MemDb hit an internal server error: {mem:?}"),
        (Ok(pg), Ok(mem)) => pg == mem,
        (Err(pg_err), Err(mem_err)) => cmp_err(pg_err, mem_err),
        _ => false,
    };
    anyhow::ensure!(is_eq, "postgres result != mem result:\n==========\nPostgres:\n{pg_res:?}\n==========\nMem:\n{mem_res:?}\n==========");
    Ok(())
}

#[cfg(feature = "_tests")]
pub async fn cmp_query_results<T: Debug + Ord + Object>(
    pg: crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>>,
    mem: crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>>,
) -> anyhow::Result<()> {
    cmp_just_errs(&pg, &mem)?;
    if !pg.is_ok() {
        return Ok(());
    }
    let pg = pg
        .unwrap()
        .collect::<Vec<crate::Result<FullObject>>>()
        .await
        .into_iter()
        .collect::<crate::Result<Vec<FullObject>>>();
    let mem = mem
        .unwrap()
        .collect::<Vec<crate::Result<FullObject>>>()
        .await
        .into_iter()
        .collect::<crate::Result<Vec<FullObject>>>();
    cmp_just_errs(&pg, &mem)?;
    if !pg.is_ok() {
        return Ok(());
    }
    let mut pg = pg
        .unwrap()
        .into_iter()
        .map(|o| o.last_snapshot::<T>())
        .collect::<anyhow::Result<Vec<Arc<T>>>>()
        .context("getting last snapshot of objects from postgres")?;
    let mut mem = mem
        .unwrap()
        .into_iter()
        .map(|o| o.last_snapshot::<T>())
        .collect::<anyhow::Result<Vec<Arc<T>>>>()
        .context("getting last snapshot of objects from mem")?;
    pg.sort_unstable();
    mem.sort_unstable();
    cmp(Ok(pg), Ok(mem))?;
    Ok(())
}
