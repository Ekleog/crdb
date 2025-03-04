use anyhow::Context;
use crdb_core::DynSized;
use std::{any::Any, fmt::Debug};

mod full_object;
mod fuzz_object_full;
mod fuzz_remote_perms;
mod fuzz_simple;
mod mem_db;
mod object_delegate_perms;
mod object_full;
mod object_perms;
mod object_simple;
mod smoke_test;
mod stubs;

pub use crdb_core::{self, Error, Result};

pub use anyhow;
pub use paste;
pub use rust_decimal;
pub use ulid;

pub use full_object::FullObject;
pub use mem_db::MemDb;
pub use object_delegate_perms::{TestEventDelegatePerms, TestObjectDelegatePerms};
pub use object_full::{TestEventFull, TestObjectFull};
pub use object_perms::{TestEventPerms, TestObjectPerms};
pub use object_simple::{TestEventSimple, TestObjectSimple};
pub use stubs::*;

crdb_macros::db! {
    pub struct Config {
        delegate_perms: TestObjectDelegatePerms,
        full: TestObjectFull,
        perms: TestObjectPerms,
        simple: TestObjectSimple,
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

pub fn cmp_err(pg: &crate::Error, mem: &crate::Error) -> bool {
    use crate::Error::*;
    match (pg, mem) {
        (MissingBinaries(a), MissingBinaries(b)) => a == b,
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
    (
        $db_type:tt,
        $( ($name:ident, $object:ident, $event:ident), )*
    ) => { $crate::paste::paste! {
        use $crate::{*, crdb_core::*};
        use std::collections::HashSet;

        #[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
        enum Op {
            $(
                [< Create $name >] {
                    object_id: ObjectId,
                    created_at: EventId,
                    object: Arc<$object>,
                    updatedness: Option<Updatedness>,
                    importance: Importance,
                },
                [< Submit $name >] {
                    object_id: usize,
                    event_id: EventId,
                    event: Arc<$event>,
                    updatedness: Option<Updatedness>,
                    additional_importance: Importance,
                },
                [< GetLatest $name >] {
                    object_id: usize,
                    importance: Importance,
                },
                // TODO(test-high): also test GetAll
                // TODO(test-high): also test query subscription / locking for client db's
                [< Query $name >] {
                    user: User,
                    only_updated_since: Option<Updatedness>,
                    query: Arc<Query>,
                },
                [< Recreate $name >] {
                    object_id: usize,
                    new_created_at: EventId,
                    object: Arc<$object>,
                    updatedness: Option<Updatedness>,
                    additional_importance: Importance,
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
            SetObjectImportance { object_id: usize, new_importance: Importance },
            SetImportanceFromQueries { object_id: usize, new_importance_from_queries: Importance },
            ClientVacuum,
            ServerVacuum { recreate_at: Option<EventId>, updatedness: Updatedness },
            // TODO(test-high): test all methods of *Db
        }

        impl Op {
            async fn apply(&self, db: &Database, s: &mut FuzzState) -> anyhow::Result<()> {
                match self {
                    $(
                        Op::[< Create $name >] {
                            object_id,
                            created_at,
                            object,
                            updatedness,
                            mut importance,
                        } => {
                            let updatedness = s.updatedness(updatedness);
                            let mut object = object.clone();
                            Arc::make_mut(&mut object).standardize(*object_id);
                            s.add_object(*object_id);
                            if s.is_server {
                                importance |= Importance::LOCK;
                            }
                            let db = db
                                .create(*object_id, *created_at, object.clone(), updatedness, importance)
                                .await;
                            let mem = s
                                .mem_db
                                .create(*object_id, *created_at, object.clone(), updatedness, importance)
                                .await;
                            cmp(db, mem)?;
                        }
                        Op::[< Submit $name >] {
                            object_id,
                            event_id,
                            event,
                            updatedness,
                            additional_importance,
                        } => {
                            let updatedness = s.updatedness(updatedness);
                            let object_id = s.object(*object_id);
                            let db = db
                                .submit::<$object>(object_id, *event_id, event.clone(), updatedness, *additional_importance)
                                .await;
                            let mem = s
                                .mem_db
                                .submit::<$object>(object_id, *event_id, event.clone(), updatedness, *additional_importance)
                                .await;
                            cmp(db, mem)?;
                        }
                        Op::[< GetLatest $name >] {
                            object_id,
                            importance,
                        } => {
                            let object_id = s.object(*object_id);
                            let db = db
                                .get_latest::<$object>(object_id, *importance)
                                .await
                                .wrap_context(&format!("getting {object_id:?} in database"));
                            let mem = s
                                .mem_db
                                .get_latest::<$object>(object_id, *importance)
                                .await
                                .wrap_context(&format!("getting {object_id:?} in mem db"));
                            cmp(db, mem)?;
                        }
                        Op::[< Query $name >] { user, only_updated_since, query } => {
                            $crate::make_fuzzer_stuffs!(@if-client $db_type {
                                let _ = user;
                                let _ = only_updated_since;
                                let db = db
                                    .client_query(*$object::type_ulid(), query.clone())
                                    .await
                                    .wrap_context("querying postgres")
                                    .map(|r| r.into_iter().collect::<HashSet<_>>());
                                let mem = s.mem_db
                                    .memdb_query::<$object>(USER_ID_NULL, None, &query)
                                    .await
                                    .wrap_context("querying mem")
                                    .map(|r| r.into_iter().collect::<HashSet<_>>());
                                cmp(db, mem)?;
                            });
                            $crate::make_fuzzer_stuffs!(@if-server $db_type {
                                let pg = db
                                    .server_query(*user, *$object::type_ulid(), *only_updated_since, query.clone())
                                    .await
                                    .wrap_context("querying postgres")
                                    .map(|r| r.into_iter().collect::<HashSet<_>>());
                                let mem = s.mem_db
                                    .memdb_query::<$object>(*user, *only_updated_since, &query)
                                    .await
                                    .map(|r| r.into_iter().collect::<HashSet<_>>());
                                cmp(pg, mem)?;
                            });
                        }
                        Op::[< Recreate $name >] {
                            object_id,
                            new_created_at,
                            object,
                            updatedness,
                            additional_importance,
                        } => {
                            $crate::make_fuzzer_stuffs!(@if-client $db_type {
                                let updatedness = s.updatedness(updatedness);
                                let object_id = s.object(*object_id);
                                let mut object = object.clone();
                                Arc::make_mut(&mut object).standardize(object_id);
                                let db = db
                                    .recreate::<$object>(
                                        object_id,
                                        *new_created_at,
                                        object.clone(),
                                        updatedness,
                                        *additional_importance,
                                    )
                                    .await;
                                let mem = s
                                    .mem_db
                                    .recreate::<$object>(
                                        object_id,
                                        *new_created_at,
                                        object.clone(),
                                        updatedness,
                                        *additional_importance,
                                    )
                                    .await;
                                cmp(db, mem)?;
                            });
                        }
                    )*
                    Op::CreateBinary { data, fake_id } => {
                        let real_hash = hash_binary(&data);
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
                        $crate::make_fuzzer_stuffs!(@if-client $db_type {
                            let object_id = s.object(*object_id);
                            let db = db.remove(object_id).await;
                            let mem = s.mem_db.remove(object_id).await;
                            cmp(db, mem)?;
                        });
                    }
                    Op::SetObjectImportance { object_id, new_importance } => {
                        $crate::make_fuzzer_stuffs!(@if-client $db_type {
                            let object_id = s.object(*object_id);
                            let db = db.set_object_importance(object_id, *new_importance).await;
                            let mem = s.mem_db.set_object_importance(object_id, *new_importance).await;
                            cmp(db, mem)?;
                        });
                    }
                    Op::SetImportanceFromQueries { object_id, new_importance_from_queries } => {
                        $crate::make_fuzzer_stuffs!(@if-client $db_type {
                            let object_id = s.object(*object_id);
                            let db = db.set_importance_from_queries(object_id, *new_importance_from_queries).await;
                            let mem = s.mem_db.set_importance_from_queries(object_id, *new_importance_from_queries).await;
                            cmp(db, mem)?;
                        });
                    }
                    Op::ClientVacuum => {
                        $crate::make_fuzzer_stuffs!(@if-client $db_type {
                            let db = db.client_vacuum(|_| (), |_| ()).await;
                            let mem = s.mem_db.client_vacuum(|_| (), |_| ()).await;
                            cmp(db, mem)?;
                        });
                    }
                    Op::ServerVacuum { recreate_at, updatedness} => {
                        $crate::make_fuzzer_stuffs!(@if-server $db_type {
                            // TODO(test-high): use MemDb's implementation of server_vacuum once it's done
                            match recreate_at {
                                None => {
                                    let db = db
                                        .server_vacuum(None, Updatedness::now(), None, |r, _| {
                                            panic!("got unexpected recreation {r:?}");
                                        })
                                        .await;
                                    let mem = s.mem_db.client_vacuum(|_| (), |_| ()).await;
                                    cmp(db, mem)?;
                                }
                                Some(_) => {
                                    let db = db
                                        .server_vacuum(*recreate_at, *updatedness, None, |_, _| {
                                            // TODO(test-high): validate that the notified recreations are the same as in memdb
                                        })
                                        .await;
                                    let mem = async move {
                                        if let Some(recreate_at) = recreate_at {
                                            $(
                                                s.mem_db.recreate_all::<$object>(*recreate_at, Some(*updatedness)).await?;
                                            )*
                                        }
                                        s.mem_db.client_vacuum(|_| (), |_| ()).await?;
                                        Ok(())
                                    }
                                    .await;
                                    cmp(db, mem)?;
                                }
                            }
                        });
                    }
                }
                Ok(())
            }
        }

        struct FuzzState {
            is_server: bool,
            ulids: Vec<Ulid>,
            mem_db: MemDb,
        }

        impl FuzzState {
            fn new(is_server: bool) -> FuzzState {
                FuzzState {
                    is_server,
                    ulids: Vec::new(),
                    mem_db: MemDb::new(is_server),
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

            fn updatedness(&self, updatedness: &Option<Updatedness>) -> Option<Updatedness> {
                if self.is_server {
                    Some(updatedness.clone().unwrap_or(Updatedness::now()))
                } else {
                    *updatedness
                }
            }
        }

        async fn fuzz_impl((cluster, is_server): &(SetupState, bool), ops: Arc<Vec<Op>>) -> Database {
            #[cfg(not(fuzzing))]
            eprintln!("Fuzzing with:\n{}", serde_json::to_string(&ops).unwrap());
            let (db, _keepalive) = make_db(cluster).await;
            let mut s = FuzzState::new(*is_server);
            for (i, op) in ops.iter().enumerate() {
                op.apply(&db, &mut s)
                    .await
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                $(
                    db.assert_invariants_for::<$object>().await;
                )*
            }
            db
        }
    } };

    (@if-client client $($t:tt)*) => { $($t)* };
    (@if-client server $($t:tt)*) => {};

    (@if-server server $($t:tt)*) => { $($t)* };
    (@if-server client $($t:tt)*) => {};
}
