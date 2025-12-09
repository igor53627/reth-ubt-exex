//! Metrics for UBT ExEx.
//!
//! Exposes Prometheus-compatible metrics for monitoring UBT performance.

use metrics::{counter, gauge, histogram};

/// Metrics labels
const METRIC_PREFIX: &str = "ubt_exex";

/// Record a block being processed
pub fn record_block_processed(block_number: u64, entries: usize, stems: usize) {
    counter!(format!("{}_blocks_processed_total", METRIC_PREFIX)).increment(1);
    histogram!(format!("{}_entries_per_block", METRIC_PREFIX)).record(entries as f64);
    gauge!(format!("{}_stems_total", METRIC_PREFIX)).set(stems as f64);
    gauge!(format!("{}_last_block_number", METRIC_PREFIX)).set(block_number as f64);
}

/// Record root computation time
pub fn record_root_computation(duration_secs: f64) {
    histogram!(format!("{}_root_computation_seconds", METRIC_PREFIX)).record(duration_secs);
}

/// Record persistence operation time
pub fn record_persistence(duration_secs: f64, stems_written: usize) {
    histogram!(format!("{}_persistence_seconds", METRIC_PREFIX)).record(duration_secs);
    histogram!(format!("{}_stems_persisted", METRIC_PREFIX)).record(stems_written as f64);
}

/// Record dirty stems gauge
pub fn record_dirty_stems(count: usize) {
    gauge!(format!("{}_dirty_stems", METRIC_PREFIX)).set(count as f64);
}

/// Record a revert operation
pub fn record_revert(blocks_reverted: usize, entries_reverted: usize) {
    counter!(format!("{}_reverts_total", METRIC_PREFIX)).increment(1);
    histogram!(format!("{}_revert_blocks", METRIC_PREFIX)).record(blocks_reverted as f64);
    histogram!(format!("{}_revert_entries", METRIC_PREFIX)).record(entries_reverted as f64);
}
