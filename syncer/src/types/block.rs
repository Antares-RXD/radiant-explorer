use serde::{Deserialize, Serialize};
use super::Transaction;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    pub hash: String,
    pub height: i64,
    pub version: i32,
    #[serde(rename = "versionHex")]
    pub version_hex: String,
    pub merkleroot: String,
    pub time: i64,
    pub mediantime: i64,
    pub nonce: u64,
    pub bits: String,
    pub difficulty: f64,
    pub chainwork: String,
    #[serde(rename = "nTx", skip_serializing_if = "Option::is_none")]
    pub n_tx: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previousblockhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nextblockhash: Option<String>,
    pub tx: Vec<Transaction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strippedsize: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<i32>,
}
