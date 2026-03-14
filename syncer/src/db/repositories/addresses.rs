use sqlx::{Postgres, Transaction as SqlxTransaction};
use std::collections::HashMap;

use crate::error::Result;
use super::to_decimal;

#[derive(Debug, Clone, Default)]
pub struct AddressDelta {
    pub balance_delta: f64,
    pub received_delta: f64,
    pub sent_delta: f64,
    pub tx_count_inc: i32,
}

pub struct AddressesRepository;

impl AddressesRepository {
    pub async fn apply_block_deltas_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        deltas: &HashMap<String, AddressDelta>,
    ) -> Result<()> {
        for (address, delta) in deltas {
            sqlx::query(
                r#"
                INSERT INTO addresses (address, balance, total_received, total_sent, tx_count)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (address) DO UPDATE SET
                    balance = addresses.balance + EXCLUDED.balance,
                    total_received = addresses.total_received + EXCLUDED.total_received,
                    total_sent = addresses.total_sent + EXCLUDED.total_sent,
                    tx_count = addresses.tx_count + EXCLUDED.tx_count
                "#,
            )
            .bind(address)
            .bind(to_decimal(delta.balance_delta))
            .bind(to_decimal(delta.received_delta))
            .bind(to_decimal(delta.sent_delta))
            .bind(delta.tx_count_inc)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    pub async fn refresh_for_addresses_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        addresses: &[String],
    ) -> Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }

        let addresses = addresses.to_vec();

        sqlx::query(
            r#"
            WITH target AS (
                SELECT DISTINCT UNNEST($1::text[]) AS address
            ),
            balances AS (
                SELECT
                    primary_address AS address,
                    SUM(value) AS total_received,
                    SUM(CASE WHEN spent_by_txid IS NOT NULL THEN value ELSE 0::DECIMAL END) AS total_sent,
                    SUM(CASE WHEN spent_by_txid IS NULL THEN value ELSE 0::DECIMAL END) AS balance
                FROM utxos
                WHERE primary_address = ANY($1::text[])
                GROUP BY primary_address
            ),
            tx_counts AS (
                SELECT
                    address,
                    COUNT(DISTINCT txid)::INTEGER AS tx_count
                FROM tx_addresses
                WHERE address = ANY($1::text[])
                GROUP BY address
            )
            INSERT INTO addresses (address, balance, total_received, total_sent, tx_count)
            SELECT
                target.address,
                COALESCE(balances.balance, 0::DECIMAL),
                COALESCE(balances.total_received, 0::DECIMAL),
                COALESCE(balances.total_sent, 0::DECIMAL),
                COALESCE(tx_counts.tx_count, 0)
            FROM target
            LEFT JOIN balances USING (address)
            LEFT JOIN tx_counts USING (address)
            ON CONFLICT (address) DO UPDATE SET
                balance = EXCLUDED.balance,
                total_received = EXCLUDED.total_received,
                total_sent = EXCLUDED.total_sent,
                tx_count = EXCLUDED.tx_count
            "#,
        )
        .bind(&addresses)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM addresses
            WHERE address = ANY($1::text[])
              AND balance = 0
              AND total_received = 0
              AND total_sent = 0
              AND tx_count = 0
            "#,
        )
        .bind(&addresses)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn apply_delta_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        address: &str,
        balance_delta: f64,
        received_delta: f64,
        sent_delta: f64,
    ) -> Result<()> {
        let balance = to_decimal(balance_delta);
        let received = to_decimal(received_delta);
        let sent = to_decimal(sent_delta);

        sqlx::query(
            r#"
            INSERT INTO addresses (address, balance, total_received, total_sent, tx_count)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (address) DO UPDATE SET
                balance = addresses.balance + $2,
                total_received = addresses.total_received + $3,
                total_sent = addresses.total_sent + $4,
                tx_count = addresses.tx_count + 1
            "#,
        )
        .bind(address)
        .bind(&balance)
        .bind(&received)
        .bind(&sent)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn upsert_credit_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        address: &str,
        value: f64,
    ) -> Result<()> {
        Self::apply_delta_tx(tx, address, value, value, 0.0).await
    }

    pub async fn upsert_debit_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        address: &str,
        value: f64,
    ) -> Result<()> {
        Self::apply_delta_tx(tx, address, -value, 0.0, value).await
    }
}

pub struct TxAddressesRepository;

impl TxAddressesRepository {
    pub async fn insert_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        txid: &str,
        address: &str,
        block_height: i64,
        block_time: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO addresses (address)
            VALUES ($1)
            ON CONFLICT (address) DO NOTHING
            "#,
        )
        .bind(address)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO tx_addresses (txid, address, block_height, time)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (txid, address) DO NOTHING
            "#,
        )
        .bind(txid)
        .bind(address)
        .bind(block_height as i32)
        .bind(block_time as i32)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub async fn affected_addresses_from_height_tx(
        tx: &mut SqlxTransaction<'_, Postgres>,
        height: i64,
    ) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT address
            FROM tx_addresses
            WHERE block_height >= $1
            "#,
        )
        .bind(height as i32)
        .fetch_all(&mut **tx)
        .await?;

        Ok(rows.into_iter().map(|(address,)| address).collect())
    }
}
