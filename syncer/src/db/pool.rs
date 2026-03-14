use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

use crate::config::DatabaseConfig;
use crate::error::Result;

pub async fn create_pool(config: &DatabaseConfig) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .connect(&config.connection_string())
        .await?;

    tracing::info!("Database connection pool created");

    sqlx::raw_sql(include_str!("../../schema.sql"))
        .execute(&pool)
        .await?;

    tracing::info!("Database schema initialized successfully");
    Ok(pool)
}
