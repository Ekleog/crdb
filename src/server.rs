use crate::User;
use anyhow::Context;
use axum::http::StatusCode;
use std::{marker::PhantomData, net::SocketAddr};

pub trait Authenticator: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(&self) -> Result<User, (StatusCode, String)>;
}

pub trait Config {}

macro_rules! generate {
    () => {
        // TODO: generate a Config impl and a (static) Db impl
    };
}

pub struct Server<C: Config> {
    _db: sqlx::PgPool,
    _phantom: PhantomData<C>,
}

impl<C: Config> Server<C> {
    pub async fn new(db_url: &str) -> anyhow::Result<Server> {
        Ok(Server {
            _db: sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // TODO: make configurable (builder pattern?)
                .connect(&db_url)
                .await
                .with_context(|| format!("opening database {db_url:?}"))?,
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
