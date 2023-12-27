use crate::{api::ServerMessage, cache::CacheDb, db_trait::ObjectId, User};
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
mod sql_db;

pub use config::Config;

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(data: Auth) -> Result<User, (StatusCode, String)>;
}

struct Session(Ulid);

pub struct Server<C: Config> {
    _config: C,
    _db: CacheDb<sql_db::SqlDb>,
    _watchers: HashMap<ObjectId, HashSet<Session>>,
    _sessions: HashMap<Session, mpsc::UnboundedSender<ServerMessage>>,
}

impl<C: Config> Server<C> {
    pub async fn new(config: C, db_url: &str, cache_watermark: usize) -> anyhow::Result<Self> {
        Ok(Server {
            _config: config,
            _db: CacheDb::new::<C::ApiConfig>(
                Arc::new(sql_db::SqlDb::connect(db_url).await?),
                cache_watermark,
            ),
            _watchers: HashMap::new(),
            _sessions: HashMap::new(),
        })
    }

    pub async fn serve(self, addr: &SocketAddr) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding to {addr:?}"))?;
        axum::serve(listener, axum::Router::new())
            .await
            .context("serving axum webserver")
    }
}
