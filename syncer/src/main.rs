mod config;
mod db;
mod error;
mod rpc;
mod sync;
mod types;

use std::sync::Arc;
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::Config;
use crate::db::create_pool;
use crate::error::Result;
use crate::rpc::RpcClient;
use crate::sync::{DailyStatsSync, MempoolSync, PriceSync, StatsSync, SyncEngine};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    init_logging();

    info!("Neurai Syncer v3.0.0 (Rust) starting...");

    // Load configuration
    let config = Arc::new(Config::load("config.json")?);
    info!(
        rpc_host = config.rpc.host,
        db_host = config.database.host,
        "Configuration loaded"
    );

    // Create database pool
    let pool = create_pool(&config.database).await?;

    // Create RPC client
    let rpc = Arc::new(RpcClient::new(&config.rpc)?);

    // Test RPC connection
    let block_count = rpc.get_block_count().await?;
    info!(block_count, "Connected to node");

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn sync tasks
    let mut handles = Vec::new();

    // Block sync engine
    {
        let config = Arc::clone(&config);
        let rpc = Arc::clone(&rpc);
        let pool = pool.clone();
        let shutdown_rx = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            let mut engine = SyncEngine::new(config, rpc, pool, shutdown_rx);
            if let Err(e) = engine.run().await {
                error!(error = %e, "Sync engine error");
            }
        }));
    }

    // Network stats sync
    {
        let config = Arc::clone(&config);
        let rpc = Arc::clone(&rpc);
        let pool = pool.clone();
        let shutdown_rx = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            let mut stats = StatsSync::new(config, rpc, pool, shutdown_rx);
            if let Err(e) = stats.run().await {
                error!(error = %e, "Stats sync error");
            }
        }));
    }

    // Mempool sync
    {
        let config = Arc::clone(&config);
        let rpc = Arc::clone(&rpc);
        let pool = pool.clone();
        let shutdown_rx = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            let mut mempool = MempoolSync::new(config, rpc, pool, shutdown_rx);
            if let Err(e) = mempool.run().await {
                error!(error = %e, "Mempool sync error");
            }
        }));
    }

    // Daily stats sync
    {
        let config = Arc::clone(&config);
        let rpc = Arc::clone(&rpc);
        let pool = pool.clone();
        let shutdown_rx = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            let mut stats = DailyStatsSync::new(config, rpc, pool, shutdown_rx);
            if let Err(e) = stats.run().await {
                error!(error = %e, "Daily stats sync error");
            }
        }));
    }

    // Price sync
    {
        let config = Arc::clone(&config);
        let pool = pool.clone();
        let shutdown_rx = shutdown_rx.clone();

        handles.push(tokio::spawn(async move {
            let mut price = PriceSync::new(config, pool, shutdown_rx);
            if let Err(e) = price.run().await {
                error!(error = %e, "Price sync error");
            }
        }));
    }

    // Wait for shutdown signal
    info!("Syncer running. Press Ctrl+C to stop.");
    shutdown_signal().await;

    // Send shutdown signal to all tasks
    info!("Initiating graceful shutdown...");
    let _ = shutdown_tx.send(true);

    // Wait for all tasks to complete
    for handle in handles {
        let _ = handle.await;
    }

    // Close database pool
    pool.close().await;

    info!("Shutdown complete");
    Ok(())
}

fn init_logging() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,reqwest=warn"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}
