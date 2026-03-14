use bigdecimal::{BigDecimal, ToPrimitive};
use sqlx::{Postgres, Transaction as SqlxTransaction};

use crate::error::{Result, SyncerError};

use super::to_decimal;

#[derive(Debug, Clone)]
pub struct StoredUtxo {
    pub value: f64,
    pub primary_address: Option<String>,
    pub raw_addresses: Vec<String>,
}

pub struct UtxosRepository;

impl UtxosRepository {
    pub async fn insert_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        txid: &str,
        vout: u32,
        block_height: i64,
        block_time: i64,
        value: f64,
        script_type: &str,
        script_hex: &str,
        raw_addresses: &[String],
        primary_address: Option<&str>,
        is_coinbase: bool,
    ) -> Result<()> {
        let value = to_decimal(value);
        let raw_addresses = serde_json::to_value(raw_addresses)?;

        sqlx::query(
            r#"
            INSERT INTO utxos (
                txid, vout, block_height, time, value, script_type, script_hex,
                raw_addresses, primary_address, spent_by_txid, spent_height, is_coinbase
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, $10)
            ON CONFLICT (txid, vout) DO UPDATE SET
                block_height = EXCLUDED.block_height,
                time = EXCLUDED.time,
                value = EXCLUDED.value,
                script_type = EXCLUDED.script_type,
                script_hex = EXCLUDED.script_hex,
                raw_addresses = EXCLUDED.raw_addresses,
                primary_address = EXCLUDED.primary_address,
                spent_by_txid = NULL,
                spent_height = NULL,
                is_coinbase = EXCLUDED.is_coinbase
            "#,
        )
        .bind(txid)
        .bind(vout as i32)
        .bind(block_height as i32)
        .bind(block_time as i32)
        .bind(&value)
        .bind(script_type)
        .bind(script_hex)
        .bind(&raw_addresses)
        .bind(primary_address)
        .bind(is_coinbase)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn spend_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        prev_txid: &str,
        prev_vout: u32,
        spending_txid: &str,
        spent_height: i64,
    ) -> Result<StoredUtxo> {
        let row: Option<(BigDecimal, Option<String>, serde_json::Value)> = sqlx::query_as(
            r#"
            UPDATE utxos
            SET spent_by_txid = $3, spent_height = $4
            WHERE txid = $1 AND vout = $2 AND spent_by_txid IS NULL
            RETURNING value, primary_address, raw_addresses
            "#,
        )
        .bind(prev_txid)
        .bind(prev_vout as i32)
        .bind(spending_txid)
        .bind(spent_height as i32)
        .fetch_optional(&mut **tx)
        .await?;

        let (value, primary_address, raw_addresses) = row.ok_or_else(|| {
            SyncerError::Sync(format!(
                "Missing or already spent UTXO for input {}:{}",
                prev_txid, prev_vout
            ))
        })?;

        let raw_addresses: Vec<String> = serde_json::from_value(raw_addresses)?;

        Ok(StoredUtxo {
            value: value.to_f64().unwrap_or_default(),
            primary_address,
            raw_addresses,
        })
    }

    pub async fn affected_addresses_from_height_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        height: i64,
    ) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT primary_address
            FROM utxos
            WHERE primary_address IS NOT NULL
              AND (block_height >= $1 OR spent_height >= $1)
            "#,
        )
        .bind(height as i32)
        .fetch_all(&mut **tx)
        .await?;

        Ok(rows.into_iter().map(|(address,)| address).collect())
    }

    pub async fn revert_from_height_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        height: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE utxos
            SET spent_by_txid = NULL,
                spent_height = NULL
            WHERE spent_height >= $1
            "#,
        )
        .bind(height as i32)
        .execute(&mut **tx)
        .await?;

        sqlx::query("DELETE FROM utxos WHERE block_height >= $1")
            .bind(height as i32)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }
}
