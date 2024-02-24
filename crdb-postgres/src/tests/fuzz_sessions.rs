use super::TmpDb;
use crate::PostgresDb;
use anyhow::Context;
use crdb_core::{NewSession, Session, SessionRef, SessionToken, SystemTimeExt, User};
use crdb_test_utils::{cmp, Config, USER_ID_1};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use ulid::Ulid;
use web_time::SystemTime;

const NANOS_PER_SEC: u128 = 1_000_000_000;

// Ignore issues that could arise after year ~4970. MarkActive will only happen with
// server-controlled systemtimes anyway.
const MAX_TIME_AHEAD: u128 = NANOS_PER_SEC * 3600 * 24 * 366 * 3000;

fn reasonable_system_time(u: &mut arbitrary::Unstructured) -> arbitrary::Result<SystemTime> {
    let d = u.arbitrary::<u128>()? % MAX_TIME_AHEAD;
    Ok(SystemTime::UNIX_EPOCH
        + Duration::new((d / NANOS_PER_SEC) as u64, (d % NANOS_PER_SEC) as u32))
}

#[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
enum Op {
    Login(NewSession),
    Resume(usize),
    MarkActive(
        usize,
        #[arbitrary(with = reasonable_system_time)] SystemTime,
    ),
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

fn floor_time(t: SystemTime) -> SystemTime {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let d = Duration::from_millis(d.as_millis() as u64);
    SystemTime::UNIX_EPOCH + d
}

async fn apply_op(db: &PostgresDb<Config>, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Login(session) => {
            let mut session = Session::new(session.clone());
            let tok = match db.login_session(session.clone()).await {
                Ok((tok, _)) => tok,
                Err(crate::Error::NullByteInString) if session.session_name.contains('\0') => {
                    return Ok(())
                }
                Err(crate::Error::InvalidTime(t))
                    if session.expiration_time == Some(t) && t.ms_since_posix().is_err() =>
                {
                    return Ok(())
                }
                Err(e) => Err(e).context("logging session in")?,
            };
            session.login_time = floor_time(session.login_time);
            session.last_active = floor_time(session.last_active);
            session.expiration_time = session.expiration_time.map(floor_time);
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
            if let Err(e) = at.ms_since_posix() {
                return cmp(at.ms_since_posix(), Err(e));
            }
            match s.sessions.get_mut(&token) {
                None => cmp(pg, Err(crate::Error::InvalidToken(token)))?,
                Some(session) => {
                    session.last_active = floor_time(*at);
                    cmp(pg, Ok(()))?;
                }
            }
        }
        Op::Rename(session, new_name) => {
            let token = s.token_for(*session);
            let pg = db.rename_session(token, new_name).await;
            if let Err(e) = crdb_core::check_string(new_name) {
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
            None => {
                db.disconnect_session(User::now(), SessionRef::now())
                    .await?
            }
            Some(token) => match s.sessions.get(token) {
                None => {
                    db.disconnect_session(User::now(), SessionRef::now())
                        .await?
                }
                Some(session) => {
                    db.disconnect_session(session.user_id, session.session_ref)
                        .await?;
                    s.sessions.remove(token);
                }
            },
        },
    }
    Ok(())
}

fn fuzz_impl(cluster: &TmpDb, ops: &Vec<Op>) {
    #[cfg(not(fuzzing))]
    eprintln!("Fuzzing with:\n{}", serde_json::to_string(&ops).unwrap());
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let pool = cluster.pool().await;
            let db = PostgresDb::connect(pool.clone(), 0).await.unwrap().0;
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
        .with_arbitrary()
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
            expiration_time: Some(SystemTime::from_ms_since_posix(i64::MAX).unwrap()),
        })],
    )
}

#[test]
fn regression_disconnect_was_ignored() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Login(NewSession {
                user_id: USER_ID_1,
                session_name: String::new(),
                expiration_time: Some(SystemTime::from_ms_since_posix(0).unwrap()),
            }),
            Disconnect(0),
            MarkActive(0, SystemTime::from_ms_since_posix(0).unwrap()),
        ],
    )
}

#[test]
fn regression_memdb_ignored_disconnect_user_param() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Login(NewSession {
                user_id: USER_ID_1,
                session_name: String::from("foo"),
                expiration_time: Some(SystemTime::from_ms_since_posix(1).unwrap()),
            }),
            Disconnect(0),
            ListSessions(0),
        ],
    );
}

#[test]
fn regression_expiration_time_too_late_caused_crash() {
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![Op::Login(NewSession {
            user_id: USER_ID_1,
            session_name: String::from(""),
            expiration_time: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(u64::MAX / 2)),
        })],
    );
}
