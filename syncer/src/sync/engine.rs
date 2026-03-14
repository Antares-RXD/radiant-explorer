use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::db::repositories::{
    AddressesRepository, BlocksRepository, SyncStateRepository, TxAddressesRepository,
    UtxosRepository,
};
use crate::db::DbPool;
use crate::error::Result;
use crate::rpc::RpcClient;

use super::cache::TransactionCache;
use super::processor::BlockProcessor;

const TX_CACHE_CAPACITY: usize = 10_000;

pub struct SyncEngine {
    config: Arc<Config>,
    rpc: Arc<RpcClient>,
    pool: DbPool,
    shutdown_rx: watch::Receiver<bool>,
}

impl SyncEngine {
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
        info!("Starting sync engine");

        let mut tx_cache = TransactionCache::new(TX_CACHE_CAPACITY);

        loop {
            // Check for shutdown signal
            if *self.shutdown_rx.borrow() {
                info!("Shutdown signal received, stopping sync engine");
                break;
            }

            match self.sync_iteration(&mut tx_cache).await {
                Ok(synced) => {
                    if !synced {
                        // No new blocks, wait before checking again
                        let wait_ms = self.config.sync.main_loop_new_block_wait;
                        sleep(Duration::from_millis(wait_ms)).await;
                    }
                }
                Err(e) => {
                    error!(error = %e, "Sync iteration failed");
                    let wait_ms = self.config.sync.main_loop_error_wait;
                    sleep(Duration::from_millis(wait_ms)).await;
                }
            }

            // Periodically clear cache to prevent memory growth
            if tx_cache.len() > TX_CACHE_CAPACITY / 2 {
                tx_cache.clear();
            }
        }

        Ok(())
    }

    async fn sync_iteration(&self, tx_cache: &mut TransactionCache) -> Result<bool> {
        let chain_height = self.rpc.get_block_count().await?;
        let db_height = SyncStateRepository::get_last_height(&self.pool)
            .await?
            .unwrap_or(-1);

        if db_height >= chain_height {
            return Ok(false); // Already synced
        }

        // Check for reorg
        if db_height >= 0 {
            self.check_and_handle_reorg(db_height).await?;
        }

        // Calculate batch range
        let from_height = db_height + 1;
        let to_height = std::cmp::min(
            from_height + self.config.sync.batch_size as i64 - 1,
            chain_height,
        );

        info!(
            from = from_height,
            to = to_height,
            chain_height,
            "Syncing blocks"
        );

        let blocks = self.fetch_blocks(from_height, to_height).await?;
        self.process_blocks(&blocks, tx_cache).await?;

        Ok(true)
    }

    async fn fetch_blocks(&self, from_height: i64, to_height: i64) -> Result<Vec<crate::types::Block>> {
        let mut blocks = Vec::with_capacity((to_height - from_height + 1) as usize);
        for height in from_height..=to_height {
            if *self.shutdown_rx.borrow() {
                break;
            }

            let block = self.rpc.get_block_by_height(height).await?;
            blocks.push(block);
        }

        Ok(blocks)
    }

    async fn process_blocks(
        &self,
        blocks: &[crate::types::Block],
        tx_cache: &mut TransactionCache,
    ) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        // Prefetch input transactions
        let processor = BlockProcessor::new(&self.rpc, &self.pool);
        processor.prefetch_inputs(blocks, tx_cache).await?;

        // Process blocks
        for block in blocks {
            if *self.shutdown_rx.borrow() {
                break;
            }

            processor.process_block(block, tx_cache).await?;
        }

        Ok(())
    }

    async fn check_and_handle_reorg(&self, db_height: i64) -> Result<()> {
        // Check last few blocks for hash mismatch
        let check_depth = std::cmp::min(10, db_height + 1);

        for i in 0..check_depth {
            let height = db_height - i;

            let db_hash = BlocksRepository::get_by_height(&self.pool, height).await?;
            let chain_hash = self.rpc.get_block_hash(height).await?;

            if let Some(ref db_h) = db_hash {
                if db_h != &chain_hash {
                    warn!(
                        height,
                        db_hash = db_h,
                        chain_hash,
                        "Reorg detected, rolling back"
                    );

                    // Rollback from this height
                    self.rollback_from_height(height).await?;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn rollback_from_height(&self, height: i64) -> Result<()> {
        info!(from_height = height, "Rolling back blocks due to reorg");

        let mut tx = self.pool.begin().await?;
        let mut affected_addresses = TxAddressesRepository::affected_addresses_from_height_tx(&mut tx, height).await?;
        affected_addresses.extend(UtxosRepository::affected_addresses_from_height_tx(&mut tx, height).await?);

        affected_addresses.sort();
        affected_addresses.dedup();

        UtxosRepository::revert_from_height_tx(&mut tx, height).await?;

        // Delete blocks from this height onwards
        // CASCADE will handle transactions, tx_addresses
        let deleted = BlocksRepository::delete_from_height(&mut tx, height).await?;

        AddressesRepository::refresh_for_addresses_tx(&mut tx, &affected_addresses).await?;

        // Update sync state
        if height > 0 {
            SyncStateRepository::set_last_height_tx(&mut tx, height - 1).await?;
        }

        tx.commit().await?;

        info!(deleted_blocks = deleted, "Rollback complete");
        Ok(())
    }
}
