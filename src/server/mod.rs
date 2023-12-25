use crate::{api::ServerMessage, db_trait::ObjectId, User};
use anyhow::Context;
use axum::http::StatusCode;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};
use tokio::sync::mpsc;
use ulid::Ulid;

#[doc(hidden)] // See comment on config::private::Sealed for why this is public
pub mod config;
pub use config::Config;

mod db;
use db::Db;

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(data: Auth) -> Result<User, (StatusCode, String)>;
}

struct Session(Ulid);

pub struct Server<C: Config> {
    _config: C,
    _db: Db,
    _watchers: HashMap<ObjectId, HashSet<Session>>,
    _sessions: HashMap<Session, mpsc::UnboundedSender<ServerMessage>>,
}

impl<C: Config> Server<C> {
    pub async fn new(config: C, db_url: &str) -> anyhow::Result<Self> {
        Ok(Server {
            _config: config,
            _db: Db::connect(db_url).await?,
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
