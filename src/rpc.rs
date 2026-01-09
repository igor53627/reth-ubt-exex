//! UBT RPC endpoints for PIR state export.
//!
//! Provides JSON-RPC endpoints for exporting UBT state in PIR2 format.
//!
//! # Endpoints
//!
//! - `ubt_exportState`: Export full UBT state to PIR2 format
//! - `ubt_exportContract`: Export single contract state
//! - `ubt_getStateDelta`: Get state changes for block range
//! - `ubt_getRoot`: Get current UBT root hash and block info

use alloy_primitives::{Address, B256};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

use crate::persistence::UbtDatabase;
use crate::key_index::KeyIndex;
use nomt::{Nomt, Options as NomtOptions};
use nomt::trie::KeyPath;
use nomt::hasher::Blake3Hasher as NomtBlake3Hasher;
use crate::pir_export;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportStateParams {
    pub output_path: String,
    #[serde(default)]
    pub chain_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportStateResult {
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockHash")]
    pub block_hash: B256,
    pub root: B256,
    #[serde(rename = "entryCount")]
    pub entry_count: u64,
    #[serde(rename = "stemCount")]
    pub stem_count: u64,
    #[serde(rename = "stateFile")]
    pub state_file: String,
    #[serde(rename = "stemIndexFile")]
    pub stem_index_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportContractParams {
    pub contract: Address,
    pub output_path: String,
    #[serde(default)]
    pub chain_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetStateDeltaParams {
    pub from_block: u64,
    pub to_block: u64,
    pub output_path: String,
    #[serde(default)]
    pub chain_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateDeltaResult {
    #[serde(rename = "fromBlock")]
    pub from_block: u64,
    #[serde(rename = "toBlock")]
    pub to_block: u64,
    #[serde(rename = "headBlock")]
    pub head_block: u64,
    #[serde(rename = "entryCount")]
    pub entry_count: u64,
    #[serde(rename = "deltaFile")]
    pub delta_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRootResult {
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    #[serde(rename = "blockHash")]
    pub block_hash: B256,
    pub root: B256,
    #[serde(rename = "stemCount")]
    pub stem_count: usize,
}

#[rpc(server, namespace = "ubt")]
pub trait UbtApi {
    #[method(name = "exportState")]
    async fn export_state(&self, params: ExportStateParams) -> RpcResult<ExportStateResult>;

    #[method(name = "exportContract")]
    async fn export_contract(&self, params: ExportContractParams) -> RpcResult<ExportStateResult>;

    #[method(name = "getStateDelta")]
    async fn get_state_delta(&self, params: GetStateDeltaParams) -> RpcResult<StateDeltaResult>;

    #[method(name = "getRoot")]
    async fn get_root(&self) -> RpcResult<GetRootResult>;
}

#[derive(Clone)]
pub struct UbtRpc {
    db: Arc<UbtDatabase>,
    default_chain_id: u64,
    delta_retention: u64,
    nomt_dir: PathBuf,
    key_index_path: PathBuf,
}

impl UbtRpc {
    pub fn new(
        db: UbtDatabase,
        default_chain_id: u64,
        delta_retention: u64,
        nomt_dir: PathBuf,
        key_index_path: PathBuf,
    ) -> Self {
        Self {
            db: Arc::new(db),
            default_chain_id,
            delta_retention,
            nomt_dir,
            key_index_path,
        }
    }

    pub fn from_paths(
        ubt_dir: PathBuf,
        nomt_dir: PathBuf,
        key_index_path: PathBuf,
        default_chain_id: u64,
        delta_retention: u64,
    ) -> Result<Self, crate::error::UbtError> {
        let db = UbtDatabase::open(&ubt_dir)?;
        Ok(Self::new(
            db,
            default_chain_id,
            delta_retention,
            nomt_dir,
            key_index_path,
        ))
    }

    fn ensure_nomt_synced(&self) -> Result<(), crate::error::UbtError> {
        const NOMT_HEAD_KEY: KeyPath = [0xff; 32];

        let mut opts = NomtOptions::new();
        opts.path(&self.nomt_dir);
        opts.rollback(true);
        opts.commit_concurrency(1);

        let nomt = Nomt::<NomtBlake3Hasher>::open(opts)
            .map_err(|e| crate::error::UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        let nomt_head = match nomt.read(NOMT_HEAD_KEY) {
            Ok(Some(val)) => {
                let bytes: [u8; 8] = val
                    .as_slice()
                    .try_into()
                    .map_err(|_| crate::error::UbtError::Database(crate::error::DatabaseError::Mdbx(
                        "Invalid NOMT head value".to_string(),
                    )))?;
                u64::from_be_bytes(bytes)
            }
            _ => 0,
        };

        let key_index = KeyIndex::open(&self.key_index_path)?;
        let key_index_head = key_index
            .load_head()?
            .ok_or_else(|| {
                crate::error::UbtError::Database(crate::error::DatabaseError::Mdbx(
                    "Missing key index head metadata".to_string(),
                ))
            })?
            .block_number;

        if nomt_head != key_index_head {
            return Err(crate::error::UbtError::Database(
                crate::error::DatabaseError::Mdbx(format!(
                    "NOMT head {} does not match key index head {}",
                    nomt_head, key_index_head
                )),
            ));
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl UbtApiServer for UbtRpc {
    async fn export_state(&self, params: ExportStateParams) -> RpcResult<ExportStateResult> {
        self.ensure_nomt_synced().map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;
        let chain_id = params.chain_id.unwrap_or(self.default_chain_id);
        let output_dir = PathBuf::from(&params.output_path);

        let result = pir_export::export_full_state_from_nomt(
            &self.db,
            &self.nomt_dir,
            &self.key_index_path,
            &output_dir,
            chain_id,
        )
        .map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;

        Ok(ExportStateResult {
            block_number: result.block_number,
            block_hash: result.block_hash,
            root: result.root,
            entry_count: result.entry_count,
            stem_count: result.stem_count,
            state_file: result.state_file,
            stem_index_file: result.stem_index_file,
        })
    }

    async fn export_contract(&self, params: ExportContractParams) -> RpcResult<ExportStateResult> {
        self.ensure_nomt_synced().map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;
        let chain_id = params.chain_id.unwrap_or(self.default_chain_id);
        let output_dir = PathBuf::from(&params.output_path);

        let result = pir_export::export_contract_state_from_nomt(
            &self.db,
            &self.nomt_dir,
            params.contract,
            &self.key_index_path,
            &output_dir,
            chain_id,
        )
        .map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;

        Ok(ExportStateResult {
            block_number: result.block_number,
            block_hash: result.block_hash,
            root: result.root,
            entry_count: result.entry_count,
            stem_count: result.stem_count,
            state_file: result.state_file,
            stem_index_file: result.stem_index_file,
        })
    }

    async fn get_state_delta(&self, params: GetStateDeltaParams) -> RpcResult<StateDeltaResult> {
        self.ensure_nomt_synced().map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;
        let chain_id = params.chain_id.unwrap_or(self.default_chain_id);
        let output_dir = PathBuf::from(&params.output_path);

        let result = pir_export::get_state_delta(
            &self.db,
            params.from_block,
            params.to_block,
            &output_dir,
            chain_id,
            self.delta_retention,
        )
        .map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;

        Ok(StateDeltaResult {
            from_block: result.from_block,
            to_block: result.to_block,
            head_block: result.head_block,
            entry_count: result.entry_count,
            delta_file: result.delta_file,
        })
    }

    async fn get_root(&self) -> RpcResult<GetRootResult> {
        let key_index = KeyIndex::open(&self.key_index_path).map_err(|e| {
            jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
        })?;
        let head = key_index
            .load_head()
            .map_err(|e| {
                jsonrpsee::types::ErrorObjectOwned::owned(-32000, e.to_string(), None::<()>)
            })?
            .ok_or_else(|| {
                jsonrpsee::types::ErrorObjectOwned::owned(
                    -32000,
                    "No canonical state yet",
                    None::<()>,
                )
            })?;

        Ok(GetRootResult {
            block_number: head.block_number,
            block_hash: head.block_hash,
            root: head.root,
            stem_count: head.stem_count as usize,
        })
    }
}
