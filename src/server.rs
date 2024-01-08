use crate::{
    api::{ApiConfig, ServerMessage},
    cache::CacheDb,
    ids, ObjectId, Timestamp, User,
};
use anyhow::Context;
use axum::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::mpsc;
use ulid::Ulid;

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(bolero::generator::TypeGenerator))]
pub struct NewSession {
    pub user_id: User,
    pub session_name: String,
    pub expiration_time: Option<Timestamp>,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub user_id: User,
    pub session_ref: SessionRef,
    pub session_name: String,
    pub login_time: Timestamp,
    pub last_active: Timestamp,
    pub expiration_time: Option<Timestamp>,
}

impl Session {
    pub fn new(s: NewSession) -> Session {
        let now = Timestamp::now();
        Session {
            user_id: s.user_id,
            session_ref: SessionRef::now(),
            session_name: s.session_name,
            login_time: now,
            last_active: now,
            expiration_time: s.expiration_time,
        }
    }
}

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(data: Auth) -> Result<Session, (StatusCode, String)>;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionToken(Ulid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionRef(Ulid);

ids::impl_for_id!(SessionToken);
ids::impl_for_id!(SessionRef);

pub struct Server<C: ServerConfig> {
    _config: C,
    db: Arc<CacheDb<PostgresDb<C>>>,
    postgres_db: Arc<PostgresDb<C>>,
    _watchers: HashMap<ObjectId, HashSet<SessionToken>>,
    _sessions: HashMap<SessionToken, mpsc::UnboundedSender<ServerMessage>>,
}

impl<C: ServerConfig> Server<C> {
    pub async fn new(config: C, db: sqlx::PgPool, cache_watermark: usize) -> anyhow::Result<Self> {
        // TODO: force configuring a vacuuming schedule
        <C::ApiConfig as ApiConfig>::check_ulids();
        let postgres_db = Arc::new(postgres_db::PostgresDb::connect(db).await?);
        Ok(Server {
            _config: config,
            db: CacheDb::new::<C::ApiConfig>(postgres_db.clone(), cache_watermark),
            postgres_db,
            _watchers: HashMap::new(),
            _sessions: HashMap::new(),
        })
    }

    pub async fn serve(&self, addr: &SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding to {addr:?}"))?;
        self.db.reduce_size_to(1024).await; // shut dead code warning up for now
        self.db.clear_cache().await; // shut dead code warning up for now
        axum::serve(listener, axum::Router::new())
            .await
            .context("serving axum webserver")
    }

    /// Cleans up and optimizes up the database
    ///
    /// After running this, the database will reject any new change that would happen before
    /// `no_new_changes_before` if it is set.
    pub async fn vacuum(
        &self,
        no_new_changes_before: Option<Timestamp>,
        kill_sessions_older_than: Option<Timestamp>,
    ) -> crate::Result<()> {
        self.postgres_db
            .vacuum(
                no_new_changes_before,
                kill_sessions_older_than,
                &*self.db,
                |_| todo!(),
            )
            .await
    }
}
