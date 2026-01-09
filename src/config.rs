//! Configuration for UBT ExEx.
//!
//! Supports both CLI arguments and environment variable fallbacks.

use clap::Args;
use std::path::PathBuf;

/// Default flush interval (blocks between MDBX writes)
pub const DEFAULT_FLUSH_INTERVAL: u64 = 1;

/// Default delta retention (blocks to keep deltas for reorgs)
pub const DEFAULT_DELTA_RETENTION: u64 = 256;
/// Default HTTP RPC address
pub const DEFAULT_RPC_HTTP_ADDR: &str = "127.0.0.1:9845";
/// Default IPC socket path
pub const DEFAULT_RPC_IPC_PATH: &str = "/tmp/ubt-exex.ipc";

/// UBT ExEx configuration arguments.
#[derive(Debug, Clone, Args)]
#[command(next_help_heading = "UBT ExEx")]
pub struct UbtConfig {
    /// Directory for UBT data storage.
    /// Falls back to RETH_DATA_DIR env var, then current directory.
    #[arg(long = "ubt.data-dir", value_name = "PATH")]
    pub data_dir: Option<PathBuf>,

    /// Number of blocks between MDBX flushes.
    /// Higher values reduce I/O but increase memory usage.
    #[arg(long = "ubt.flush-interval", value_name = "BLOCKS", default_value_t = DEFAULT_FLUSH_INTERVAL)]
    pub flush_interval: u64,

    /// Number of blocks to retain deltas for reorg handling.
    /// Older deltas are pruned to save disk space.
    #[arg(long = "ubt.delta-retention", value_name = "BLOCKS", default_value_t = DEFAULT_DELTA_RETENTION)]
    pub delta_retention: u64,

    /// Disable UBT ExEx (useful for debugging).
    #[arg(long = "ubt.disable", default_value_t = false)]
    pub disabled: bool,

    /// HTTP RPC listen address (set to \"off\" to disable).
    #[arg(long = "ubt.rpc-http", value_name = "ADDR")]
    pub rpc_http_addr: Option<String>,

    /// IPC socket path (set to \"off\" to disable).
    #[arg(long = "ubt.rpc-ipc", value_name = "PATH")]
    pub rpc_ipc_path: Option<PathBuf>,
}

impl UbtConfig {
    /// Get the data directory, with env var fallback.
    ///
    /// Precedence: CLI arg > RETH_DATA_DIR env var > current directory
    pub fn get_data_dir(&self) -> PathBuf {
        self.data_dir.clone().unwrap_or_else(|| {
            std::env::var("RETH_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
        })
    }

    /// Get flush interval, with env var fallback for backwards compatibility.
    ///
    /// Precedence: CLI arg (if not default) > UBT_FLUSH_INTERVAL env var > default
    pub fn get_flush_interval(&self) -> u64 {
        if self.flush_interval != DEFAULT_FLUSH_INTERVAL {
            return self.flush_interval;
        }
        match std::env::var("UBT_FLUSH_INTERVAL") {
            Ok(s) => s.parse().unwrap_or_else(|_| {
                tracing::warn!(value = %s, "Invalid UBT_FLUSH_INTERVAL, using default");
                self.flush_interval
            }),
            Err(_) => self.flush_interval,
        }
    }

    /// Get delta retention, with env var fallback for backwards compatibility.
    ///
    /// Precedence: CLI arg (if not default) > UBT_DELTA_RETENTION env var > default
    pub fn get_delta_retention(&self) -> u64 {
        if self.delta_retention != DEFAULT_DELTA_RETENTION {
            return self.delta_retention;
        }
        match std::env::var("UBT_DELTA_RETENTION") {
            Ok(s) => s.parse().unwrap_or_else(|_| {
                tracing::warn!(value = %s, "Invalid UBT_DELTA_RETENTION, using default");
                self.delta_retention
            }),
            Err(_) => self.delta_retention,
        }
    }

    /// Get HTTP RPC address with env var fallback.
    pub fn get_rpc_http_addr(&self) -> Option<String> {
        if let Some(addr) = &self.rpc_http_addr {
            return normalize_optional(addr);
        }
        if let Ok(addr) = std::env::var("UBT_RPC_HTTP_ADDR") {
            return normalize_optional(&addr);
        }
        Some(DEFAULT_RPC_HTTP_ADDR.to_string())
    }

    /// Get IPC socket path with env var fallback.
    pub fn get_rpc_ipc_path(&self) -> Option<PathBuf> {
        if let Some(path) = &self.rpc_ipc_path {
            return normalize_optional(path.to_string_lossy().as_ref())
                .map(PathBuf::from);
        }
        if let Ok(path) = std::env::var("UBT_RPC_IPC_PATH") {
            return normalize_optional(&path).map(PathBuf::from);
        }
        Some(PathBuf::from(DEFAULT_RPC_IPC_PATH))
    }

    /// Create a config for testing with explicit data directory.
    ///
    /// Uses flush_interval=1 and delta_retention=1024 for predictable test behavior.
    #[cfg(test)]
    pub fn for_tests(data_dir: std::path::PathBuf) -> Self {
        Self {
            data_dir: Some(data_dir),
            flush_interval: 1,
            delta_retention: 1024,
            disabled: false,
            rpc_http_addr: Some(DEFAULT_RPC_HTTP_ADDR.to_string()),
            rpc_ipc_path: Some(PathBuf::from(DEFAULT_RPC_IPC_PATH)),
        }
    }
}

impl Default for UbtConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            delta_retention: DEFAULT_DELTA_RETENTION,
            disabled: false,
            rpc_http_addr: None,
            rpc_ipc_path: None,
        }
    }
}

fn normalize_optional(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_ascii_lowercase();
    if lowered == "off" || lowered == "0" || lowered == "false" || lowered == "none" {
        return None;
    }
    Some(trimmed.to_string())
}
