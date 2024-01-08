use super::TmpDb;
use crate::{
    server::PostgresDb,
    test_utils::{cmp, db::ServerConfig, USER_ID_1},
    NewSession, Session, SessionToken, Timestamp,
};
use anyhow::Context;
use std::collections::HashMap;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Login(NewSession),
    Resume(usize),
}

struct FuzzState {
    tokens: Vec<SessionToken>,
    sessions: HashMap<SessionToken, Session>,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            tokens: Vec::new(),
            sessions: HashMap::new(),
        }
    }
}

async fn apply_op(db: &PostgresDb<ServerConfig>, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Login(session) => {
            let session = Session::new(session.clone());
            let tok = match db.login_session(session.clone()).await {
                Ok((tok, _)) => tok,
                Err(crate::Error::NullByteInString) if session.session_name.contains('\0') => {
                    return Ok(())
                }
                Err(crate::Error::InvalidTimestamp(t))
                    if session.expiration_time == Some(t) && t.time_ms() > i64::MAX as u64 =>
                {
                    return Ok(());
                }
                Err(e) => Err(e).context("logging session in")?,
            };
            anyhow::ensure!(
                s.sessions.insert(tok, session).is_none(),
                "db returned a session token conflict"
            );
            s.tokens.push(tok);
        }
        Op::Resume(session) => {
            let token = s
                .tokens
                .get(*session)
                .copied()
                .unwrap_or_else(SessionToken::new);
            let pg = db.resume_session(token).await;
            match s.sessions.get(&token) {
                None => cmp(pg, Err(crate::Error::InvalidToken(token)))?,
                Some(session) => cmp(pg, Ok(session.clone()))?,
            }
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

#[test]
fn regression_postgres_rejected_null_bytes_in_string() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![Login(NewSession {
            user_id: USER_ID_1,
            session_name: String::from("foo\0bar"),
            expiration_time: None,
        })],
    )
}

#[test]
fn regression_too_big_timestamp_led_to_crash() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![Login(NewSession {
            user_id: USER_ID_1,
            session_name: String::new(),
            expiration_time: Some(Timestamp::from_ms(u64::MAX)),
        })],
    )
}
