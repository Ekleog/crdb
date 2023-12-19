use anyhow::Context;
use std::net::SocketAddr;

pub struct Server {
    _db: sqlx::PgPool,
}

impl Server {
    pub async fn new(db_url: &str) -> anyhow::Result<Server> {
        Ok(Server {
            _db: sqlx::postgres::PgPoolOptions::new()
                .max_connections(50) // TODO: make configurable (builder pattern?)
                .connect(&db_url)
                .await
                .with_context(|| format!("opening database {db_url:?}"))?,
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
