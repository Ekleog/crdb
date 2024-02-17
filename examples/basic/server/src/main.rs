use anyhow::Context;
use axum::{
    extract::{State, WebSocketUpgrade},
    routing::{get, post},
    Json,
};
use basic_api::{db::ServerConfig, AuthInfo};
use crdb::SessionToken;
use std::{str::FromStr, sync::Arc, time::Duration};
use tower_http::trace::TraceLayer;

const SERVER_ADDR: &str = "127.0.0.1:3000";
const CACHE_SIZE: usize = 32 * 1024 * 1024;
const RECREATE_OLDER_THAN: Duration = Duration::from_secs(5 * 60);
const KILL_SESSIONS_OLDER_THAN: Duration = Duration::from_secs(24 * 3600);

// Make sure that the code works fine with multi-threading enabled
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Open the database
    let db_url = "postgres:///basic-crdb";
    let db = crdb::sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .connect(&db_url)
        .await
        .with_context(|| format!("opening database {db_url:?}"))?;

    // Start the CRDB server
    let (crdb_server, upgrade_finished) = crdb::Server::new(
        ServerConfig,
        db,
        CACHE_SIZE,
        // Vacuum every 2 minutes, recreating objects older than 5 minutes
        crdb::ServerVacuumSchedule::new(
            crdb::cron::Schedule::from_str("0 */2 * * * * *").unwrap(),
            crdb::chrono::Utc,
        )
        .recreate_older_than(RECREATE_OLDER_THAN)
        .kill_sessions_older_than(KILL_SESSIONS_OLDER_THAN),
    )
    .await
    .context("creating crdb server")?;
    let errors_while_upgrading = upgrade_finished
        .await
        .context("waiting for upgrade to finish")?;
    anyhow::ensure!(
        errors_while_upgrading == 0,
        "got {errors_while_upgrading} errors while upgrading"
    );

    // Start the HTTP server
    let app = axum::Router::new()
        .route("/api/ws", get(websocket_handler))
        .route("/api/login", post(login))
        .layer(TraceLayer::new_for_http())
        .with_state(Arc::new(crdb_server));
    let listener = tokio::net::TcpListener::bind(SERVER_ADDR)
        .await
        .context("listening on port")?;
    tracing::info!("listening on {SERVER_ADDR}");
    axum::serve(listener, app)
        .await
        .context("serving axum webserver")
}

pub async fn login(
    State(db): State<Arc<crdb::Server<ServerConfig>>>,
    Json(data): Json<AuthInfo>,
) -> Result<Json<SessionToken>, String> {
    unimplemented!() // TODO(example-high)
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(db): State<Arc<crdb::Server<ServerConfig>>>,
) -> Result<axum::response::Response, String> {
    Ok(ws.on_upgrade(move |sock| async move {
        unimplemented!() // TODO(example-high)
    }))
}
