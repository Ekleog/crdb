use crate::User;
use anyhow::Context;
use axum::http::StatusCode;
use std::{marker::PhantomData, net::SocketAddr};

pub trait Authenticator: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(&self) -> Result<User, (StatusCode, String)>;
}

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait Config {}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_server {
    () => {
        struct ServerConfig;

        impl $crate::server::Config for ServerConfig {}
    };
}

struct Db {
    _db: sqlx::PgPool,
}
// TODO: impl (Can)ApplyCallbacks for Db

pub struct Server<C: Config> {
    _config: C,
    _db: Db,
}

impl<C: Config> Server<C> {
    pub async fn new(config: C, db_url: &str) -> anyhow::Result<Self> {
        Ok(Server {
            _config: config,
            _db: Db {
                _db: sqlx::postgres::PgPoolOptions::new()
                    .max_connections(50) // TODO: make configurable (builder pattern?)
                    .connect(&db_url)
                    .await
                    .with_context(|| format!("opening database {db_url:?}"))?,
            },
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
