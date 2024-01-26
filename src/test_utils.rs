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
macro_rules! make_fuzzer_stuffs {
    ( $( ($name:ident, $object:ident, $event:ident), )* ) => { paste::paste! {
        #[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
        enum Op {
            $(
                [< Create $name >] {
                    object_id: ObjectId,
                    created_at: EventId,
                    object: Arc<$object>,
                    lock: bool,
                },
                [< Submit $name >] {
                    object_id: usize,
                    event_id: EventId,
                    event: Arc<$event>,
                },
                [< GetLatest $name >] {
                    object_id: usize,
                    lock: bool,
                },
                // TODO(test): also test GetAll
                [< Query $name >] {
                    user: User,
                    // TODO(test): figure out a way to test only_updated_since
                    q: Query,
                },
                [< Recreate $name >] {
                    object_id: usize,
                    new_created_at: EventId,
                    object: Arc<$object>,
                    force_lock: bool,
                },
            )*
            CreateBinary {
                data: Arc<[u8]>,
                fake_id: Option<BinPtr>,
            },
            GetBinary {
                binary_id: usize,
            },
            Remove { object_id: usize },
            Unlock { object_id: usize },
            Vacuum { recreate_at: Option<Timestamp> },
        }

        impl Op {
            async fn apply(&self, db: &Database, s: &mut FuzzState) -> anyhow::Result<()> {
                match self {
                    $(
                        Op::[< Create $name >] {
                            object_id,
                            created_at,
                            object,
                            mut lock,
                        } => {
                            let mut object = object.clone();
                            Arc::make_mut(&mut object).standardize(*object_id);
                            s.add_object(*object_id);
                            lock |= s.is_server;
                            let db = db
                                .create(*object_id, *created_at, object.clone(), lock, db)
                                .await;
                            let mem = s
                                .mem_db
                                .create(*object_id, *created_at, object.clone(), lock, &s.mem_db)
                                .await;
                            cmp(db, mem)?;
                        }
                        Op::[< Submit $name >] {
                            object_id,
                            event_id,
                            event,
                        } => {
                            let object_id = s.object(*object_id);
                            let db = db
                                .submit::<$object, _>(object_id, *event_id, event.clone(), db)
                                .await;
                            let mem = s
                                .mem_db
                                .submit::<$object, _>(object_id, *event_id, event.clone(), &s.mem_db)
                                .await;
                            cmp(db, mem)?;
                        }
                        Op::[< GetLatest $name >] {
                            object_id,
                            mut lock,
                        } => {
                            let object_id = s.object(*object_id);
                            lock |= s.is_server;
                            let db = db
                                .get_latest::<$object>(lock, object_id)
                                .await
                                .wrap_context(&format!("getting {object_id:?} in database"));
                            let mem = s
                                .mem_db
                                .get_latest::<$object>(lock, object_id)
                                .await
                                .wrap_context(&format!("getting {object_id:?} in mem db"));
                            cmp(db, mem)?;
                        }
                        Op::[< Query $name >] { user, q } => {
                            run_query::<$object>(&db, &s.mem_db, *user, None, q).await?;
                        }
                        Op::[< Recreate $name >] {
                            object_id,
                            new_created_at,
                            object,
                            force_lock,
                        } => {
                            if !s.is_server {
                                let object_id = s.object(*object_id);
                                let mut object = object.clone();
                                Arc::make_mut(&mut object).standardize(object_id);
                                let db = db
                                    .recreate::<$object, _>(
                                        object_id,
                                        *new_created_at,
                                        object.clone(),
                                        *force_lock,
                                        db,
                                    )
                                    .await;
                                let mem = s
                                    .mem_db
                                    .recreate::<$object, _>(
                                        object_id,
                                        *new_created_at,
                                        object.clone(),
                                        *force_lock,
                                        &s.mem_db,
                                    )
                                    .await;
                                cmp(db, mem)?;
                            }
                        }
                    )*
                    Op::CreateBinary { data, fake_id } => {
                        let real_hash = crdb::hash_binary(&data);
                        s.add_binary(real_hash);
                        let binary_id = match fake_id {
                            Some(id) => {
                                s.add_binary(*id);
                                *id
                            }
                            None => real_hash,
                        };
                        let mem = s.mem_db.create_binary(binary_id, data.clone()).await.wrap_context("creating binary");
                        let pg = db.create_binary(binary_id, data.clone()).await.wrap_context("creating binary");
                        cmp(pg, mem)?;
                    }
                    Op::GetBinary { binary_id } => {
                        let binary_id = s.binary(*binary_id);
                        let mem = s.mem_db.get_binary(binary_id).await.wrap_context("getting binary");
                        let pg = db.get_binary(binary_id).await.wrap_context("getting binary");
                        cmp(pg, mem)?;
                    }
                    Op::Remove { object_id } => {
                        if !s.is_server {
                            let object_id = s.object(*object_id);
                            let db = db.remove(object_id).await;
                            let mem = s.mem_db.remove(object_id).await;
                            cmp(db, mem)?;
                        }
                    }
                    Op::Unlock { object_id } => {
                        if !s.is_server {
                            let object_id = s.object(*object_id);
                            let db = db.unlock(object_id).await;
                            let mem = s.mem_db.unlock(object_id).await;
                            cmp(db, mem)?;
                        }
                    }
                    Op::Vacuum { recreate_at } => {
                        run_vacuum(&db, &s.mem_db, *recreate_at).await?;
                    }
                }
                Ok(())
            }
        }

        struct FuzzState {
            is_server: bool,
            ulids: Vec<Ulid>,
            mem_db: test_utils::MemDb,
        }

        impl FuzzState {
            fn new(is_server: bool) -> FuzzState {
                FuzzState {
                    is_server,
                    ulids: Vec::new(),
                    mem_db: test_utils::MemDb::new(is_server),
                }
            }

            fn add_object(&mut self, id: ObjectId) {
                self.ulids.push(id.0)
            }

            fn add_binary(&mut self, id: BinPtr) {
                self.ulids.push(id.0)
            }

            fn object(&self, id: usize) -> ObjectId {
                #[cfg(target_arch = "wasm32")]
                let id = id % (self.ulids.len() + 1); // make valid inputs more likely
                self.ulids.get(id).copied().map(ObjectId).unwrap_or_else(ObjectId::now)
            }

            fn binary(&self, id: usize) -> BinPtr {
                #[cfg(target_arch = "wasm32")]
                let id = id % (self.ulids.len() + 1); // make valid inputs more likely
                self.ulids.get(id).copied().map(BinPtr).unwrap_or_else(BinPtr::now)
            }
        }

        async fn fuzz_impl((cluster, is_server): &(SetupState, bool), ops: Arc<Vec<Op>>) -> Database {
            let db = make_db(cluster).await;
            let mut s = FuzzState::new(*is_server);
            for (i, op) in ops.iter().enumerate() {
                op.apply(&db, &mut s)
                    .await
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                db.assert_invariants_for::<TestObjectSimple>().await;
            }
            db
        }
    } };
}
