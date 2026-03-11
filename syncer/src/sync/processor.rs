use futures::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use crate::db::repositories::{
    AddressAssetsRepository, AddressesRepository, AssetsRepository, BlocksRepository,
    SyncStateRepository, TransactionsRepository, TxAddressesRepository,
};
use crate::db::DbPool;
use crate::error::Result;
use crate::rpc::RpcClient;
use crate::types::{Block, Transaction};

use super::cache::TransactionCache;

const INPUT_FETCH_CONCURRENCY: usize = 20;

pub struct BlockProcessor<'a> {
    rpc: &'a RpcClient,
    pool: &'a DbPool,
}

impl<'a> BlockProcessor<'a> {
    pub fn new(rpc: &'a RpcClient, pool: &'a DbPool) -> Self {
        Self { rpc, pool }
    }

    pub async fn process_block(
        &self,
        block: &Block,
        tx_cache: &mut TransactionCache,
    ) -> Result<()> {
        let height = block.height;
        debug!(height, tx_count = block.tx.len(), "Processing block");

        let mut db_tx = self.pool.begin().await?;

        // 1. Insert Block
        BlocksRepository::insert_tx(&mut db_tx, block).await?;

        // 2. Process Transactions
        for transaction in &block.tx {
            let total_output = transaction.total_output();

            // Insert Transaction record
            TransactionsRepository::insert_tx(
                &mut db_tx,
                transaction,
                height,
                block.time,
                total_output,
            )
            .await?;

            // Process Inputs (Debits)
            let enriched = self
                .process_inputs(&mut db_tx, transaction, block, tx_cache)
                .await?;

            // Update raw_data with enriched transaction
            TransactionsRepository::update_raw_data_tx(&mut db_tx, &transaction.txid, &enriched)
                .await?;

            // Process Outputs (Credits)
            self.process_outputs(&mut db_tx, transaction, block).await?;
        }

        // Update Sync State
        SyncStateRepository::set_last_height_tx(&mut db_tx, height).await?;

        db_tx.commit().await?;

        if height % 100 == 0 {
            info!(height, "Block synced");
        }

        Ok(())
    }

    async fn process_inputs(
        &self,
        db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: &Transaction,
        block: &Block,
        tx_cache: &mut TransactionCache,
    ) -> Result<Transaction> {
        let mut enriched = transaction.clone();

        for (i, vin) in transaction.vin.iter().enumerate() {
            if vin.is_coinbase() {
                continue;
            }

            let txid = match &vin.txid {
                Some(t) => t,
                None => continue,
            };

            let vout_idx = match vin.vout {
                Some(v) => v as usize,
                None => continue,
            };

            // Get previous transaction from cache
            let prev_tx = match tx_cache.get(txid) {
                Some(tx) => tx,
                None => {
                    warn!(txid = txid, "Missing prevTx in cache");
                    continue;
                }
            };

            let prev_out = match prev_tx.vout.get(vout_idx) {
                Some(out) => out,
                None => continue,
            };

            let addresses = &prev_out.script_pub_key.addresses;
            let addr = match addresses {
                Some(addrs) if !addrs.is_empty() => &addrs[0],
                _ => continue,
            };

            let val = prev_out.value;

            // Enrich vin data
            enriched.vin[i].addresses = Some(vec![addr.clone()]);
            enriched.vin[i].value = Some(val);

            if val > 0.0 {
                // Standard RXD Debit
                AddressesRepository::upsert_debit_tx(db_tx, addr, val).await?;

                // Asset Debit
                if let Some(ref asset) = prev_out.script_pub_key.asset {
                    AddressAssetsRepository::upsert_debit_tx(db_tx, addr, &asset.name, asset.amount)
                        .await?;
                }

                // Index for History
                self.insert_tx_address(db_tx, &transaction.txid, addr, block).await?;
            }
        }

        Ok(enriched)
    }

    async fn process_outputs(
        &self,
        db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: &Transaction,
        block: &Block,
    ) -> Result<()> {
        for vout in &transaction.vout {
            let addresses = &vout.script_pub_key.addresses;
            let addr = match addresses {
                Some(addrs) if !addrs.is_empty() => &addrs[0],
                _ => continue,
            };

            let val = vout.value;

            if val >= 0.0 {
                // Standard RXD Credit
                AddressesRepository::upsert_credit_tx(db_tx, addr, val).await?;

                // Asset Processing
                if let Some(ref asset) = vout.script_pub_key.asset {
                    let script_type = &vout.script_pub_key.script_type;

                    // Register New/Updated Asset Metadata
                    if script_type == "new_asset" || script_type == "reissue_asset" {
                        AssetsRepository::upsert_tx(
                            db_tx,
                            asset,
                            script_type,
                            block.height,
                            &transaction.txid,
                        )
                        .await?;
                    }

                    // Credit User Balance
                    AddressAssetsRepository::upsert_credit_tx(db_tx, addr, &asset.name, asset.amount)
                        .await?;
                }

                // Index for History
                self.insert_tx_address(db_tx, &transaction.txid, addr, block).await?;
            }
        }

        Ok(())
    }

    async fn insert_tx_address(
        &self,
        db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        txid: &str,
        address: &str,
        block: &Block,
    ) -> Result<()> {
        TxAddressesRepository::insert_tx(db_tx, txid, address, block.height, block.time).await
    }

    pub async fn prefetch_inputs(
        &self,
        blocks: &[Block],
        tx_cache: &mut TransactionCache,
    ) -> Result<()> {
        debug!(block_count = blocks.len(), "Pre-fetching inputs");

        // Collect unique txids to fetch
        let mut txids_to_fetch = std::collections::HashSet::new();

        for block in blocks {
            for tx in &block.tx {
                for vin in &tx.vin {
                    if !vin.is_coinbase() {
                        if let Some(ref txid) = vin.txid {
                            txids_to_fetch.insert(txid.clone());
                        }
                    }
                }
            }
        }

        let txid_vec: Vec<String> = txids_to_fetch.into_iter().collect();
        debug!(count = txid_vec.len(), "Unique inputs to fetch");

        // Fetch transactions with bounded concurrency
        let results: Vec<Option<Transaction>> = stream::iter(txid_vec)
            .map(|txid| async move {
                match self.rpc.get_raw_transaction(&txid).await {
                    Ok(tx) => Some(tx),
                    Err(e) => {
                        warn!(txid = txid, error = %e, "Failed to fetch transaction");
                        None
                    }
                }
            })
            .buffer_unordered(INPUT_FETCH_CONCURRENCY)
            .collect()
            .await;

        // Insert into cache
        for tx in results.into_iter().flatten() {
            tx_cache.insert(tx.txid.clone(), tx);
        }

        debug!(cache_size = tx_cache.len(), "Inputs pre-fetched");

        Ok(())
    }
}
