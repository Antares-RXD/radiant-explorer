use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiningInfo {
    pub blocks: i64,
    pub difficulty: f64,
    pub networkhashps: f64,
    pub pooledtx: i32,
    pub chain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: i64,
    pub addr: String,
    #[serde(default)]
    pub addrbind: Option<String>,
    #[serde(default)]
    pub addrlocal: Option<String>,
    pub services: String,
    pub relaytxes: bool,
    pub lastsend: i64,
    pub lastrecv: i64,
    pub bytessent: i64,
    pub bytesrecv: i64,
    pub conntime: i64,
    pub timeoffset: i64,
    #[serde(default)]
    pub pingtime: Option<f64>,
    #[serde(default)]
    pub minping: Option<f64>,
    pub version: i64,
    pub subver: String,
    pub inbound: bool,
    #[serde(default)]
    pub addnode: Option<bool>,
    pub startingheight: i64,
    #[serde(default)]
    pub banscore: Option<i32>,
    #[serde(default)]
    pub synced_headers: Option<i64>,
    #[serde(default)]
    pub synced_blocks: Option<i64>,
    #[serde(default)]
    pub inflight: Option<Vec<i64>>,
    #[serde(default)]
    pub whitelisted: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxOutSetInfo {
    pub height: i64,
    pub bestblock: String,
    pub transactions: i64,
    pub txouts: i64,
    pub bogosize: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_serialized_2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash_serialized: Option<String>,
    pub disk_size: i64,
    pub total_amount: f64,
}
