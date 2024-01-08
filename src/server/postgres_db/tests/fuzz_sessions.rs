use super::TmpDb;
use crate::{
    server::{NewSession, PostgresDb, Session, SessionToken},
    test_utils::db::ServerConfig,
};
use anyhow::Context;
use std::collections::HashMap;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Login(NewSession),
}

struct FuzzState {
    sessions: HashMap<SessionToken, Session>,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            sessions: HashMap::new(),
        }
    }
}

async fn apply_op(db: &PostgresDb<ServerConfig>, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Login(session) => {
            let session = Session::new(session.clone());
            let (tok, _) = db.login_session(session.clone()).await?;
            assert!(
                s.sessions.insert(tok, session).is_none(),
                "db returned a session token conflict"
            );
        }
    }
    Ok(())
}

fn fuzz_impl(cluster: &TmpDb, ops: &Vec<Op>) {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let pool = cluster.pool().await;
            let db = PostgresDb::connect(pool.clone()).await.unwrap();
            sqlx::query(include_str!("../cleanup-db.sql"))
                .execute(&pool)
                .await
                .unwrap();
            let mut s = FuzzState::new();
            for (i, op) in ops.iter().enumerate() {
                apply_op(&db, &mut s, op)
                    .await
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
            }
        });
}

#[test]
fn fuzz() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_type()
        .for_each(move |ops| fuzz_impl(&cluster, ops))
}
