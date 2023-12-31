use super::TmpDb;
use crate::{
    server::PostgresDb,
    test_utils::{cmp, db::ServerConfig, USER_ID_1},
    NewSession, Session, SessionRef, SessionToken, Timestamp, User,
};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Login(NewSession),
    Resume(usize),
    MarkActive(usize, Timestamp),
    Rename(usize, String),
    ListSessions(usize),
    Disconnect(usize),
}

struct FuzzState {
    tokens: Vec<SessionToken>,
    sessions: HashMap<SessionToken, Session>,
    users: Vec<User>,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            tokens: Vec::new(),
            sessions: HashMap::new(),
            users: Vec::new(),
        }
    }

    fn token_for(&self, token: usize) -> SessionToken {
        self.tokens
            .get(token)
            .copied()
            .unwrap_or_else(SessionToken::new)
    }

    fn add_user(&mut self, user: User) {
        if !self.users.contains(&user) {
            self.users.push(user);
        }
    }

    fn user_for(&self, user: usize) -> User {
        self.users
            .get(user)
            .copied()
            .unwrap_or_else(|| User(Ulid::new()))
    }

    fn sessions_for(&self, user: User) -> HashSet<Session> {
        let mut res = HashSet::new();
        for s in self.sessions.values() {
            if s.user_id == user {
                res.insert(s.clone());
            }
        }
        res
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
                s.sessions.insert(tok, session.clone()).is_none(),
                "db returned a session token conflict"
            );
            s.tokens.push(tok);
            s.add_user(session.user_id);
        }
        Op::Resume(session) => {
            let token = s.token_for(*session);
            let pg = db.resume_session(token).await;
            match s.sessions.get(&token) {
                None => cmp(pg, Err(crate::Error::InvalidToken(token)))?,
                Some(session) => cmp(pg, Ok(session.clone()))?,
            }
        }
        Op::MarkActive(session, at) => {
            let token = s.token_for(*session);
            let pg = db.mark_session_active(token, *at).await;
            if let Err(e) = at.time_ms_i() {
                return cmp(at.time_ms_i(), Err(e));
            }
            match s.sessions.get_mut(&token) {
                None => cmp(pg, Err(crate::Error::InvalidToken(token)))?,
                Some(session) => {
                    session.last_active = *at;
                    cmp(pg, Ok(()))?;
                }
            }
        }
        Op::Rename(session, new_name) => {
            let token = s.token_for(*session);
            let pg = db.rename_session(token, new_name).await;
            if let Err(e) = crate::check_string(new_name) {
                return cmp(pg, Err(e));
            }
            match s.sessions.get_mut(&token) {
                None => cmp(pg, Err(crate::Error::InvalidToken(token)))?,
                Some(session) => {
                    session.session_name = new_name.clone();
                    cmp(pg, Ok(()))?;
                }
            }
        }
        Op::ListSessions(user) => {
            let user = s.user_for(*user);
            let pg = db
                .list_sessions(user)
                .await?
                .into_iter()
                .collect::<HashSet<_>>();
            anyhow::ensure!(pg == s.sessions_for(user));
        }
        Op::Disconnect(session) => match s.tokens.get(*session) {
            None => db.disconnect_session(SessionRef::now()).await?,
            Some(token) => {
                db.disconnect_session(s.sessions.get(token).unwrap().session_ref)
                    .await?;
                s.sessions.remove(token);
            }
        },
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
