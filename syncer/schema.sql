-- Schema bootstrap for fresh explorer deployments.

CREATE TABLE IF NOT EXISTS blocks (
    height INTEGER PRIMARY KEY,
    hash TEXT UNIQUE NOT NULL,
    time INTEGER NOT NULL,
    difficulty DECIMAL,
    tx_count INTEGER,
    raw_data JSONB
);
CREATE INDEX IF NOT EXISTS idx_blocks_time ON blocks (time DESC);

CREATE TABLE IF NOT EXISTS transactions (
    txid TEXT PRIMARY KEY,
    block_height INTEGER REFERENCES blocks(height) ON DELETE CASCADE,
    time INTEGER,
    total_output DECIMAL,
    raw_data JSONB
);
CREATE INDEX IF NOT EXISTS idx_tx_height ON transactions (block_height);

CREATE TABLE IF NOT EXISTS addresses (
    address TEXT PRIMARY KEY,
    balance DECIMAL NOT NULL DEFAULT 0,
    total_received DECIMAL NOT NULL DEFAULT 0,
    total_sent DECIMAL NOT NULL DEFAULT 0,
    tx_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_addr_balance ON addresses (balance DESC);

CREATE TABLE IF NOT EXISTS utxos (
    txid TEXT NOT NULL,
    vout INTEGER NOT NULL,
    block_height INTEGER NOT NULL,
    time INTEGER NOT NULL,
    value DECIMAL NOT NULL,
    script_type TEXT NOT NULL,
    script_hex TEXT NOT NULL,
    raw_addresses JSONB NOT NULL DEFAULT '[]'::jsonb,
    primary_address TEXT,
    spent_by_txid TEXT,
    spent_height INTEGER,
    is_coinbase BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (txid, vout)
);
CREATE INDEX IF NOT EXISTS idx_utxos_primary_unspent ON utxos (primary_address, spent_by_txid);
CREATE INDEX IF NOT EXISTS idx_utxos_block_height ON utxos (block_height);
CREATE INDEX IF NOT EXISTS idx_utxos_spent_height ON utxos (spent_height);

CREATE TABLE IF NOT EXISTS tx_addresses (
    txid TEXT REFERENCES transactions(txid) ON DELETE CASCADE,
    address TEXT REFERENCES addresses(address) ON DELETE CASCADE,
    block_height INTEGER,
    time INTEGER,
    PRIMARY KEY (txid, address)
);
CREATE INDEX IF NOT EXISTS idx_txaddr_address ON tx_addresses (address);
CREATE INDEX IF NOT EXISTS idx_txaddr_time ON tx_addresses (time DESC);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS network_stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    difficulty DECIMAL,
    hashrate DECIMAL,
    connections INTEGER,
    height INTEGER,
    supply DECIMAL,
    price_usd DECIMAL,
    market_cap_usd DECIMAL,
    peers_data JSONB,
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS mempool (
    txid TEXT PRIMARY KEY,
    time INTEGER,
    raw_data JSONB
);
CREATE INDEX IF NOT EXISTS idx_mempool_time ON mempool (time DESC);

CREATE TABLE IF NOT EXISTS assets (
    name TEXT PRIMARY KEY,
    type TEXT,
    amount DECIMAL,
    units INTEGER,
    reissuable BOOLEAN,
    has_ipfs BOOLEAN,
    ipfs_hash TEXT,
    block_height INTEGER,
    txid TEXT
);
CREATE INDEX IF NOT EXISTS idx_assets_name ON assets (name);

CREATE TABLE IF NOT EXISTS address_assets (
    address TEXT REFERENCES addresses(address) ON DELETE CASCADE,
    asset_name TEXT REFERENCES assets(name) ON DELETE CASCADE,
    balance DECIMAL NOT NULL DEFAULT 0,
    PRIMARY KEY (address, asset_name)
);
CREATE INDEX IF NOT EXISTS idx_addr_asset_bal ON address_assets (asset_name, balance DESC);

CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    tx_count INTEGER NOT NULL DEFAULT 0,
    total_output DECIMAL NOT NULL DEFAULT 0,
    sum_difficulty DECIMAL NOT NULL DEFAULT 0,
    block_count INTEGER NOT NULL DEFAULT 0,
    new_assets_count INTEGER NOT NULL DEFAULT 0,
    active_address_count INTEGER NOT NULL DEFAULT 0,
    burned_coins DECIMAL NOT NULL DEFAULT 0,
    sum_block_size BIGINT NOT NULL DEFAULT 0,
    new_supply DECIMAL NOT NULL DEFAULT 0
);
