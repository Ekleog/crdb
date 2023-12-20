use crate::User;
use anyhow::Context;
use axum::http::StatusCode;
use std::{net::SocketAddr, marker::PhantomData};

pub trait Authenticator<Auth>: for<'a> serde::Deserialize<'a> + serde::Serialize {
    fn authenticate(data: Auth) -> Result<User, (StatusCode, String)>;
}

// This module needs to actually be public, because `generate_server!` needs to be
// able to implement Config. However, making it doc(hidden) makes it look as though
// it is actually sealed in the documentation, which is good because we don't want
// users to rely on any stability guarantees there.
#[doc(hidden)]
pub mod private {
    #[doc(hidden)]
    pub trait Sealed {}
}

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait Config<Auth>: private::Sealed {}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_server {
    ( $auth:ty | $($object:ty),* ) => {
        pub struct ServerConfig;

        impl $crate::server::Config<$auth> for ServerConfig {}
    };
}

struct Db {
    _db: sqlx::PgPool,
}
// TODO: impl (Can)ApplyCallbacks for Db

pub struct Server<Auth, C: Config<Auth>> {
    _config: C,
    _db: Db,
    _phantom: PhantomData<Auth>,
}

impl<Auth, C: Config<Auth>> Server<Auth, C> {
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
