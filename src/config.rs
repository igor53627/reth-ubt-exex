//! Configuration for UBT ExEx.
//!
//! Supports both CLI arguments and environment variable fallbacks.

use clap::Args;
use std::path::PathBuf;

/// Default flush interval (blocks between MDBX writes)
pub const DEFAULT_FLUSH_INTERVAL: u64 = 1;

/// Default delta retention (blocks to keep deltas for reorgs)
pub const DEFAULT_DELTA_RETENTION: u64 = 256;

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
}

impl UbtConfig {
    /// Get the data directory, with env var fallback.
    pub fn get_data_dir(&self) -> PathBuf {
        self.data_dir.clone().unwrap_or_else(|| {
            std::env::var("RETH_DATA_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
        })
    }

    /// Get flush interval, with env var fallback for backwards compatibility.
    pub fn get_flush_interval(&self) -> u64 {
        if self.flush_interval != DEFAULT_FLUSH_INTERVAL {
            return self.flush_interval;
        }
        std::env::var("UBT_FLUSH_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.flush_interval)
    }
}

impl Default for UbtConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            delta_retention: DEFAULT_DELTA_RETENTION,
            disabled: false,
        }
    }
}
