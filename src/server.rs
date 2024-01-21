use crate::{
    api::{ApiConfig, ServerMessage},
    cache::CacheDb,
    ObjectId, Session, SessionToken, Timestamp,
};
use anyhow::Context;
use axum::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::{sync::mpsc, task::JoinHandle};

mod config;
mod postgres_db;

pub use self::postgres_db::{ComboLock, PostgresDb};
pub use config::ServerConfig;

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    // TODO(api): make sure protocol is two-step, with server sending random data to client and
    // only then client answering with the Authenticator? This'd make it possible to use
    // a public key to auth
    fn authenticate(data: Auth) -> Result<Session, (StatusCode, String)>;
}

pub struct Server<C: ServerConfig> {
    _config: C,
    db: Arc<CacheDb<PostgresDb<C>>>,
    postgres_db: Arc<PostgresDb<C>>,
    _watchers: HashMap<ObjectId, HashSet<SessionToken>>,
    _sessions: HashMap<SessionToken, mpsc::UnboundedSender<ServerMessage>>,
}

impl<C: ServerConfig> Server<C> {
    /// Returns both the server itself, as well as a `JoinHandle` that will resolve once all the operations
    /// needed for database upgrading are over. The handle resolves with the number of errors that occurred
    /// during the upgrade, normal runs would return 0. There will be one error message in the tracing logs
    /// for each such error.
    pub async fn new(
        config: C,
        db: sqlx::PgPool,
        cache_watermark: usize,
    ) -> anyhow::Result<(Self, JoinHandle<usize>)> {
        // TODO(server): force configuring a vacuuming schedule
        <C::ApiConfig as ApiConfig>::check_ulids();
        let postgres_db = Arc::new(postgres_db::PostgresDb::connect(db).await?);
        let upgrade_handle = tokio::task::spawn(C::reencode_old_versions(postgres_db.clone()));
        let this = Server {
            _config: config,
            db: CacheDb::new::<C::ApiConfig>(postgres_db.clone(), cache_watermark),
            postgres_db,
            _watchers: HashMap::new(),
            _sessions: HashMap::new(),
        };
        Ok((this, upgrade_handle))
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
                |_| unimplemented!(), // TODO(api)
            )
            .await
    }
}
