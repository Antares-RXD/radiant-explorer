use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptSig {
    pub asm: String,
    pub hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub name: String,
    pub amount: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub units: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reissuable: Option<i32>,
    #[serde(rename = "hasIPFS", skip_serializing_if = "Option::is_none")]
    pub has_ipfs: Option<i32>,
    #[serde(rename = "ipfs_hash", skip_serializing_if = "Option::is_none")]
    pub ipfs_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptPubKey {
    pub asm: String,
    pub hex: String,
    #[serde(rename = "reqSigs", skip_serializing_if = "Option::is_none")]
    pub req_sigs: Option<i32>,
    #[serde(rename = "type")]
    pub script_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<Asset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vin {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vout: Option<u32>,
    #[serde(rename = "scriptSig", skip_serializing_if = "Option::is_none")]
    pub script_sig: Option<ScriptSig>,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coinbase: Option<String>,
    // Enriched fields (added during processing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
}

impl Vin {
    pub fn is_coinbase(&self) -> bool {
        self.coinbase.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vout {
    pub value: f64,
    pub n: u32,
    #[serde(rename = "scriptPubKey")]
    pub script_pub_key: ScriptPubKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub txid: String,
    pub hash: String,
    pub version: i32,
    pub size: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vsize: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<i32>,
    pub locktime: u64,
    pub vin: Vec<Vin>,
    pub vout: Vec<Vout>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hex: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blockhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmations: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocktime: Option<i64>,
}

impl Transaction {
    pub fn total_output(&self) -> f64 {
        self.vout.iter().map(|v| v.value).sum()
    }
}
