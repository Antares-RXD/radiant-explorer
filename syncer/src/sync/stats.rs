use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::db::repositories::NetworkStatsRepository;
use crate::db::DbPool;
use crate::error::Result;
use crate::rpc::RpcClient;

use super::run_interval_loop;

pub struct StatsSync {
    config: Arc<Config>,
    rpc: Arc<RpcClient>,
    pool: DbPool,
    shutdown_rx: watch::Receiver<bool>,
}

impl StatsSync {
    pub fn new(
        config: Arc<Config>,
        rpc: Arc<RpcClient>,
        pool: DbPool,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            rpc,
            pool,
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting network stats sync");

        run_interval_loop(
            &self.shutdown_rx,
            self.config.sync.network_stats_interval,
            || async { self.sync_stats().await },
            "Failed to sync network stats",
            "Shutdown signal received, stopping stats sync",
        )
        .await
    }

    async fn sync_stats(&self) -> Result<()> {
        debug!("Syncing network stats");

        // Fetch mining info
        let mining_info = self.rpc.get_mining_info().await?;

        // Fetch connection count
        let connections = self.rpc.get_connection_count().await?;

        // Fetch peer info
        let peers = self.rpc.get_peer_info().await?;

        // Fetch UTXO set info for supply
        let txout_info = self.rpc.get_tx_out_set_info().await?;

        // Update database
        NetworkStatsRepository::update(
            &self.pool,
            mining_info.difficulty,
            mining_info.networkhashps,
            connections,
            mining_info.blocks,
            txout_info.total_amount,
            &peers,
        )
        .await?;

        debug!(
            height = mining_info.blocks,
            difficulty = mining_info.difficulty,
            connections,
            "Network stats updated"
        );

        Ok(())
    }
}

pub struct PriceSync {
    config: Arc<Config>,
    pool: DbPool,
    client: reqwest::Client,
    shutdown_rx: watch::Receiver<bool>,
}

impl PriceSync {
    pub fn new(
        config: Arc<Config>,
        pool: DbPool,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            pool,
            client: reqwest::Client::new(),
            shutdown_rx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting price sync");

        run_interval_loop(
            &self.shutdown_rx,
            self.config.api.price_fetch_interval,
            || async { self.sync_price().await },
            "Failed to sync price",
            "Shutdown signal received, stopping price sync",
        )
        .await
    }

    async fn sync_price(&self) -> Result<()> {
        debug!("Fetching price from CoinGecko");

        let price = match self.fetch_price_usd().await {
            Ok(price) => price,
            Err(e) => {
                warn!(error = %e, "Failed to fetch price from CoinGecko");
                return Ok(());
            }
        };

        let supply = NetworkStatsRepository::get_supply(&self.pool).await?;
        let market_cap = supply * price;

        if supply <= 0.0 {
            warn!(supply, "Supply not available yet; market cap will be 0");
        }

        info!(
            supply,
            price_usd = price,
            market_cap_usd = market_cap,
            "Computed market cap (supply * price)"
        );
        NetworkStatsRepository::update_price(&self.pool, price, market_cap).await?;

        info!(price_usd = price, market_cap_usd = market_cap, "Price updated");

        Ok(())
    }

    async fn fetch_price_usd(&self) -> Result<f64> {
        #[derive(serde::Deserialize)]
        struct CoinGeckoResponse {
            radiant: Option<CoinGeckoPrice>,
        }

        #[derive(serde::Deserialize)]
        struct CoinGeckoPrice {
            usd: Option<f64>,
        }

        let response = self
            .client
            .get(&self.config.api.coingecko_url)
            .header("User-Agent", "radiant-syncer/3.0")
            .header("Accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(crate::error::SyncerError::Config(format!(
                "CoinGecko response status: {}",
                response.status()
            )));
        }

        let data: CoinGeckoResponse = response.json().await?;
        let price = data.radiant.and_then(|n| n.usd).unwrap_or(0.0);

        if price <= 0.0 {
            return Err(crate::error::SyncerError::Config(
                "CoinGecko returned empty price".to_string(),
            ));
        }

        Ok(price)
    }
}
