//! Sluice server: gRPC ingestion and the REST job API in one process.
//!
//! Configuration (environment variables):
//! - `SLUICE_GRPC_ADDR` — gRPC ingest address (default `0.0.0.0:50051`)
//! - `SLUICE_HTTP_ADDR` — REST API address (default `0.0.0.0:8080`)
//! - `SLUICE_DATA_DIR` — where checkpoints are stored (default `./data`)
//! - `SLUICE_UI_DIR` — built frontend to serve (default `frontend/dist`, if present)
//! - `RUST_LOG` — log filter (default `info`)

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use sluice_api::JobManager;
use sluice_ingest::EventRouter;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

fn addr_from_env(var: &str, default: &str) -> Result<SocketAddr, Box<dyn Error>> {
    let raw = std::env::var(var).unwrap_or_else(|_| default.to_owned());
    raw.parse().map_err(|e| format!("{var}={raw}: {e}").into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let grpc_addr = addr_from_env("SLUICE_GRPC_ADDR", "0.0.0.0:50051")?;
    let http_addr = addr_from_env("SLUICE_HTTP_ADDR", "0.0.0.0:8080")?;

    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .map_err(|e| format!("binding gRPC address {grpc_addr}: {e}"))?;
    let http_listener = TcpListener::bind(http_addr)
        .await
        .map_err(|e| format!("binding HTTP address {http_addr}: {e}"))?;

    let router = EventRouter::new();
    let data_dir = std::env::var("SLUICE_DATA_DIR").unwrap_or_else(|_| "data".to_owned());
    let manager = Arc::new(JobManager::new(router.clone(), &data_dir));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
        let _ = shutdown_tx.send(true);
    });
    let shutdown = |mut rx: watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|stop| *stop).await;
    };

    tracing::info!(%grpc_addr, "gRPC ingest listening");
    tracing::info!(%http_addr, "REST API listening");
    tracing::info!(data_dir = %data_dir, "checkpoint storage");

    let grpc = sluice_ingest::serve(grpc_listener, router, shutdown(shutdown_rx.clone()));
    let ui_dir = std::path::PathBuf::from(std::env::var("SLUICE_UI_DIR").unwrap_or_else(|_| "frontend/dist".to_owned()));
    let ui_dir = ui_dir.join("index.html").is_file().then_some(ui_dir);
    match &ui_dir {
        Some(dir) => tracing::info!(ui = %dir.display(), "serving the web UI"),
        None => tracing::info!("no built web UI found (frontend/dist); serving the API only"),
    }
    let http = sluice_api::serve(http_listener, manager, ui_dir, shutdown(shutdown_rx));

    let (grpc, http) = tokio::join!(grpc, http);
    grpc?;
    http?;
    Ok(())
}
