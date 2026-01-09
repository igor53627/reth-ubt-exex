# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- PIR state export RPC endpoints (#31)
  - `ubt_exportState`: Export full UBT state to PIR2 format for inspire-exex
  - `ubt_exportContract`: Export single contract state
  - `ubt_getStateDelta`: Get state changes for block range (incremental PIR updates)
  - `ubt_getRoot`: Get current UBT root hash and block info
- JSON-RPC server wiring with IPC (default `/tmp/ubt-exex.ipc`) and localhost HTTP
- Criterion benchmarks for MDBX persistence performance
- Stem-to-address reverse mapping table (`ubt_stem_addresses`)
  - Enables recovery of address from stem for PIR2 export
  - Populated automatically for new stems during commit
- `PendingEntry` struct for tracking address with tree entries
- PIR2 format export module (`pir_export.rs`)
  - StateHeader (64 bytes): magic, version, entry count, block info
  - StorageEntry (84 bytes): address, tree_index, value
  - Stem index file for O(log N) lookups

### Fixed
- Critical bug: dirty overlay now seeds from MDBX before mutation (#27)
  - Previously, updating a stem not in dirty_stems created an empty StemNode
  - This caused all other subindex values in MDBX to be lost on flush
  - Now loads existing stem from MDBX to preserve all subindex values
- Error handling: MDBX read errors are now propagated instead of silently swallowed (#27)
  - Replaced `.ok().flatten()` patterns with proper `?` error propagation

### Added
- Property-based testing for critical invariants (#28)
  - Delta apply/revert restores original state
  - Revert then replay produces same final state
  - Root hash determinism (order independence)
  - Overlay + MDBX matches HashMap model
  - Stem count monotonically increasing
  - Multi-block reorg correctness
- Production readiness improvements (#13):
  - Custom error types with `thiserror` (#18)
  - Prometheus-compatible metrics (#16)
  - CLI configuration support via clap (#17)
  - Delta pruning for finalized blocks (#19)
  - Graceful shutdown with state flush (#20)
  - Comprehensive test suite for persistence layer (#15)
- `shutdown()` method for graceful ExEx termination
- `prune_deltas_before()` for bounded delta storage
- `UbtConfig` struct for configuration management

### Changed
- Dependencies now use git URLs instead of local paths (#14)
- Added `.cargo/config.toml` for local development overrides
- Improved documentation throughout (#21)

### Performance
- Deferred root hash computation (#1) - `rebuild_root()` now called once per `root_hash()` instead of on every insert
  - Reduces per-block CPU from O(N * S log S) to O(S log S) where N=entries, S=stems
  - `root_hash()` now takes `&mut self` (breaking API change in ubt crate)
- Streaming root verification at startup (#6) - uses StreamingTreeBuilder for memory-efficient verification
- MDBX-backed read path (#5) - overlay-aware getters for proof generation without full tree
- Batch persistence across blocks (#2) - MDBX writes batched by `UBT_FLUSH_INTERVAL` env var
  - Default interval=1 maintains current behavior
  - Higher values reduce I/O when same stems modified across consecutive blocks
- Memory optimization (#23, #24, #25) - removed 80GB+ RAM requirement
  - Full tree no longer loaded at startup - MDBX is canonical state store
  - All reads via dirty overlay + MDBX fallback
  - Root computed via streaming from MDBX (memory spike during computation only)
  - Baseline memory now <1GB instead of ~80GB+ for Sepolia
- Parallel stem hashing (#7) - uses rayon for multi-core root computation
  - Enabled `parallel` feature in ubt dependency
  - Switched to `build_root_hash_parallel` for both root computation and verification
  - Stem hashing distributed across cores, tree building remains sequential

### Fixed
- Proper reorg handling with per-block deltas (#3)
  - Stores old values before applying state changes
  - Reverts apply deltas in LIFO order (newest block first, within-block reversed)
  - Deltas for persisted blocks preserved for crash recovery

### Known Issues
- Root computation still creates Vec of all entries (memory spike during flush)
- Root only computed on flush, not per-block (last_root returned between flushes)

## [0.1.0] - 2024-12-08

### Added
- Initial release
- UBT ExEx plugin for reth
- MDBX persistence for stem nodes
- Basic account/storage/code tracking
- Integration with `ubt` crate
