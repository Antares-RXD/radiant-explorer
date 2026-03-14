use futures::stream::{self, StreamExt};
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, warn};

use crate::db::repositories::{
    AddressAssetsRepository, AddressDelta, AddressesRepository, AssetsRepository, BlocksRepository,
    SyncStateRepository, TransactionsRepository, TxAddressesRepository, UtxosRepository,
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
        let mut address_deltas: HashMap<String, AddressDelta> = HashMap::new();
        let mut tx_seen_addresses: HashSet<(String, String)> = HashSet::new();

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
                .process_inputs(
                    &mut db_tx,
                    transaction,
                    block,
                    tx_cache,
                    &mut address_deltas,
                    &mut tx_seen_addresses,
                )
                .await?;

            // Update raw_data with enriched transaction
            TransactionsRepository::update_raw_data_tx(&mut db_tx, &transaction.txid, &enriched)
                .await?;

            // Process Outputs (Credits)
            self.process_outputs(
                &mut db_tx,
                transaction,
                block,
                &mut address_deltas,
                &mut tx_seen_addresses,
            )
            .await?;

            tx_cache.insert(transaction.txid.clone(), transaction.clone());
        }

        AddressesRepository::apply_block_deltas_tx(&mut db_tx, &address_deltas).await?;

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
        address_deltas: &mut HashMap<String, AddressDelta>,
        tx_seen_addresses: &mut HashSet<(String, String)>,
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

            let spent_utxo =
                UtxosRepository::spend_tx(db_tx, txid, vout_idx as u32, &transaction.txid, block.height)
                    .await?;

            let prev_tx = tx_cache.get(txid);
            let prev_out = prev_tx.and_then(|tx| tx.vout.get(vout_idx));
            let val = spent_utxo.value;

            // Enrich vin data
            enriched.vin[i].addresses = (!spent_utxo.raw_addresses.is_empty())
                .then_some(spent_utxo.raw_addresses.clone());
            enriched.vin[i].value = Some(val);

            if val > 0.0 {
                if let Some(addr) = spent_utxo.primary_address.as_ref() {
                    let delta = address_deltas.entry(addr.clone()).or_default();
                    delta.balance_delta -= val;
                    delta.sent_delta += val;
                }

                // Asset Debit
                if let (Some(prev_out), Some(addr)) = (prev_out, spent_utxo.primary_address.as_deref()) {
                    if let Some(ref asset) = prev_out.script_pub_key.asset {
                        AddressAssetsRepository::upsert_debit_tx(db_tx, addr, &asset.name, asset.amount)
                            .await?;
                    }
                }

                // Index for History
                self.insert_tx_addresses(
                    db_tx,
                    &transaction.txid,
                    &spent_utxo.raw_addresses,
                    block,
                    tx_seen_addresses,
                    address_deltas,
                )
                .await?;
            }
        }

        Ok(enriched)
    }

    async fn process_outputs(
        &self,
        db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transaction: &Transaction,
        block: &Block,
        address_deltas: &mut HashMap<String, AddressDelta>,
        tx_seen_addresses: &mut HashSet<(String, String)>,
    ) -> Result<()> {
        let is_coinbase = transaction.vin.iter().any(|vin| vin.is_coinbase());

        for vout in &transaction.vout {
            let raw_addresses = vout.script_pub_key.addresses.clone().unwrap_or_default();
            let primary_address = if raw_addresses.len() == 1 {
                raw_addresses.first().map(String::as_str)
            } else {
                None
            };

            let val = vout.value;

            if val >= 0.0 {
                if let Some(addr) = primary_address {
                    let delta = address_deltas.entry(addr.to_string()).or_default();
                    delta.balance_delta += val;
                    delta.received_delta += val;
                }

                UtxosRepository::insert_tx(
                    db_tx,
                    &transaction.txid,
                    vout.n,
                    block.height,
                    block.time,
                    val,
                    &vout.script_pub_key.script_type,
                    &vout.script_pub_key.hex,
                    &raw_addresses,
                    primary_address,
                    is_coinbase,
                )
                .await?;

                // Asset Processing
                if let (Some(ref asset), Some(addr)) =
                    (&vout.script_pub_key.asset, raw_addresses.first())
                {
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
                self.insert_tx_addresses(
                    db_tx,
                    &transaction.txid,
                    &raw_addresses,
                    block,
                    tx_seen_addresses,
                    address_deltas,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn insert_tx_addresses(
        &self,
        db_tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        txid: &str,
        addresses: &[String],
        block: &Block,
        tx_seen_addresses: &mut HashSet<(String, String)>,
        address_deltas: &mut HashMap<String, AddressDelta>,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        for address in addresses {
            if seen.insert(address) {
                TxAddressesRepository::insert_tx(db_tx, txid, address, block.height, block.time)
                    .await?;

                let key = (txid.to_string(), address.clone());
                if tx_seen_addresses.insert(key) {
                    address_deltas.entry(address.clone()).or_default().tx_count_inc += 1;
                }
            }
        }

        Ok(())
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
