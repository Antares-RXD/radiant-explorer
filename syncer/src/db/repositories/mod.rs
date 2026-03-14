mod blocks;
mod transactions;
mod addresses;
mod assets;
mod sync_state;
mod network_stats;
mod mempool;
mod daily_stats;
mod utxos;

pub use blocks::BlocksRepository;
pub use transactions::TransactionsRepository;
pub use addresses::{AddressDelta, AddressesRepository, TxAddressesRepository};
pub use assets::{AssetsRepository, AddressAssetsRepository};
pub use sync_state::SyncStateRepository;
pub use network_stats::NetworkStatsRepository;
pub use mempool::MempoolRepository;
pub use daily_stats::DailyStatsRepository;
pub use utxos::{StoredUtxo, UtxosRepository};

use bigdecimal::BigDecimal;

pub(crate) fn to_decimal(value: f64) -> BigDecimal {
    BigDecimal::try_from(value).unwrap_or_default()
}
