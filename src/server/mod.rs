use crate::User;
use anyhow::Context;
use axum::http::StatusCode;
use std::{marker::PhantomData, net::SocketAddr};

#[doc(hidden)] // See comment on config::private::Sealed for why this is public
pub mod config;
pub use config::Config;

mod db;
use db::Db;

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(data: Auth) -> Result<User, (StatusCode, String)>;
}

pub struct Server<Auth, C: Config<Auth>> {
    _config: C,
    _db: Db,
    _phantom: PhantomData<Auth>,
}

impl<Auth, C: Config<Auth>> Server<Auth, C> {
    pub async fn new(config: C, db_url: &str) -> anyhow::Result<Self> {
        Ok(Server {
            _config: config,
            _db: Db::connect(db_url).await?,
            _phantom: PhantomData,
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
