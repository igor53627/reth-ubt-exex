//! PIR2 state export for inspire-exex integration.
//!
//! This module exports UBT state in PIR2 format for use with the inspire PIR server.
//! The format is designed for O(log N) index lookups via a stem offset table.
//!
//! # File Formats
//!
//! ## state.bin (PIR2 format)
//! ```text
//! Header (64 bytes):
//!   magic: "PIR2" (4 bytes)
//!   version: u16 (2 bytes, little-endian)
//!   entry_size: u16 (2 bytes, little-endian, always 84)
//!   entry_count: u64 (8 bytes, little-endian)
//!   block_number: u64 (8 bytes, little-endian)
//!   chain_id: u64 (8 bytes, little-endian)
//!   block_hash: [u8; 32]
//!
//! Entries (84 bytes each, sorted by tree_key):
//!   address: [u8; 20]
//!   tree_index: [u8; 32]
//!   value: [u8; 32]
//! ```
//!
//! ## stem-index.bin
//! ```text
//! Header: stem_count (u64, 8 bytes, little-endian)
//! Entries (39 bytes each):
//!   stem: [u8; 31]
//!   offset: u64 (8 bytes, little-endian)
//! ```

use alloy_primitives::{Address, B256};
use nomt::{Nomt, Options as NomtOptions};
use nomt::hasher::Blake3Hasher as NomtBlake3Hasher;
use nomt::trie::KeyPath;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use tracing::info;
use ubt::Stem;

use crate::error::{Result, UbtError};
use crate::key_index::KeyIndex;
use crate::persistence::UbtDatabase;

pub const STATE_MAGIC: [u8; 4] = *b"PIR2";
pub const STATE_HEADER_SIZE: usize = 64;
pub const STATE_ENTRY_SIZE: usize = 84;
pub const STEM_INDEX_ENTRY_SIZE: usize = 39;

#[derive(Debug, Clone)]
pub struct StateHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub entry_size: u16,
    pub entry_count: u64,
    pub block_number: u64,
    pub chain_id: u64,
    pub block_hash: [u8; 32],
}

impl StateHeader {
    pub fn new(entry_count: u64, block_number: u64, chain_id: u64, block_hash: B256) -> Self {
        Self {
            magic: STATE_MAGIC,
            version: 1,
            entry_size: STATE_ENTRY_SIZE as u16,
            entry_count,
            block_number,
            chain_id,
            block_hash: block_hash.0,
        }
    }

    pub fn to_bytes(&self) -> [u8; STATE_HEADER_SIZE] {
        let mut buf = [0u8; STATE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.entry_size.to_le_bytes());
        buf[8..16].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.block_number.to_le_bytes());
        buf[24..32].copy_from_slice(&self.chain_id.to_le_bytes());
        buf[32..64].copy_from_slice(&self.block_hash);
        buf
    }
}

#[derive(Debug, Clone)]
pub struct StorageEntry {
    pub address: [u8; 20],
    pub tree_index: [u8; 32],
    pub value: [u8; 32],
}

impl StorageEntry {
    pub fn to_bytes(&self) -> [u8; STATE_ENTRY_SIZE] {
        let mut buf = [0u8; STATE_ENTRY_SIZE];
        buf[0..20].copy_from_slice(&self.address);
        buf[20..52].copy_from_slice(&self.tree_index);
        buf[52..84].copy_from_slice(&self.value);
        buf
    }
}

#[derive(Debug, Clone)]
pub struct ExportResult {
    pub block_number: u64,
    pub block_hash: B256,
    pub root: B256,
    pub entry_count: u64,
    pub stem_count: u64,
    pub state_file: String,
    pub stem_index_file: String,
}

pub fn export_full_state(
    db: &UbtDatabase,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    export_full_state_from_mdbx(db, output_dir, chain_id)
}

pub fn export_full_state_from_mdbx(
    db: &UbtDatabase,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    let head = db.load_head()?.ok_or_else(|| {
        UbtError::Database(crate::error::DatabaseError::Mdbx(
            "No canonical state yet".to_string(),
        ))
    })?;

    info!(
        block = head.block_number,
        root = %head.root,
        stems = head.stem_count,
        "Exporting UBT state to PIR2 format"
    );

    std::fs::create_dir_all(output_dir)?;

    let state_path = output_dir.join("state.bin");
    let stem_index_path = output_dir.join("stem-index.bin");

    let state_file = File::create(&state_path)?;
    let mut state_writer = BufWriter::new(state_file);

    let placeholder_header = StateHeader::new(0, head.block_number, chain_id, head.block_hash);
    state_writer.write_all(&placeholder_header.to_bytes())?;

    let mut entry_offset: u64 = 0;
    let mut stem_index_entries: Vec<(Stem, u64)> = Vec::new();
    let mut missing_addresses: Vec<Stem> = Vec::new();

    for (stem, stem_node) in db.iter_stems()? {
        let address = match db.load_stem_address(&stem)? {
            Some(addr) => addr,
            None => {
                missing_addresses.push(stem);
                continue;
            }
        };

        let mut subindices: Vec<_> = stem_node.values.keys().copied().collect();
        subindices.sort_unstable();

        if subindices.is_empty() {
            continue;
        }

        let start_offset = entry_offset;

        for subindex in subindices {
            let value = stem_node.values[&subindex];
            let tree_index = tree_index_from_key(&stem, subindex);

            let entry = StorageEntry {
                address: address.0 .0,
                tree_index,
                value: value.0,
            };

            state_writer.write_all(&entry.to_bytes())?;
            entry_offset += 1;
        }

        stem_index_entries.push((stem, start_offset));
    }

    if !missing_addresses.is_empty() {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "Missing stem->address mappings for {} stems. PIR export requires a fresh UBT build.",
                missing_addresses.len()
            ),
        )));
    }

    state_writer.flush()?;
    let mut state_file = state_writer
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    state_file.seek(SeekFrom::Start(0))?;
    let final_header = StateHeader::new(entry_offset, head.block_number, chain_id, head.block_hash);
    state_file.write_all(&final_header.to_bytes())?;
    state_file.flush()?;

    let stem_index_file = File::create(&stem_index_path)?;
    let mut stem_writer = BufWriter::new(stem_index_file);

    let stem_count = stem_index_entries.len() as u64;
    stem_writer.write_all(&stem_count.to_le_bytes())?;

    for (stem, offset) in &stem_index_entries {
        stem_writer.write_all(stem.as_bytes())?;
        stem_writer.write_all(&offset.to_le_bytes())?;
    }
    stem_writer.flush()?;

    info!(
        entries = entry_offset,
        stems = stem_count,
        state_file = %state_path.display(),
        stem_index_file = %stem_index_path.display(),
        "PIR2 export complete"
    );

    Ok(ExportResult {
        block_number: head.block_number,
        block_hash: head.block_hash,
        root: head.root,
        entry_count: entry_offset,
        stem_count,
        state_file: state_path.display().to_string(),
        stem_index_file: stem_index_path.display().to_string(),
    })
}

pub fn export_full_state_from_nomt(
    _db: &UbtDatabase,
    nomt_dir: &Path,
    key_index_path: &Path,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    let key_index = KeyIndex::open(key_index_path)?;
    let head = key_index.load_head()?.ok_or_else(|| {
        UbtError::Database(crate::error::DatabaseError::Mdbx(
            "Missing key index head metadata".to_string(),
        ))
    })?;

    info!(
        block = head.block_number,
        root = %head.root,
        stems = head.stem_count,
        "Exporting UBT state to PIR2 format (NOMT values)"
    );

    std::fs::create_dir_all(output_dir)?;

    let state_path = output_dir.join("state.bin");
    let stem_index_path = output_dir.join("stem-index.bin");

    let state_file = File::create(&state_path)?;
    let mut state_writer = BufWriter::new(state_file);

    let placeholder_header = StateHeader::new(0, head.block_number, chain_id, head.block_hash);
    state_writer.write_all(&placeholder_header.to_bytes())?;

    let nomt = open_nomt(nomt_dir)?;

    let mut entry_offset: u64 = 0;
    let mut stem_index_entries: Vec<(Stem, u64)> = Vec::new();

    key_index.for_each_stem(|stem, record| {
        let mut has_entries = false;
        let start_offset = entry_offset;

        for subindex in iter_bitmap_subindices(&record.bitmap) {
            let tree_index = tree_index_from_key(&stem, subindex);
            let value = read_nomt_value(&nomt, tree_index)?;

            let entry = StorageEntry {
                address: record.address.into_array(),
                tree_index,
                value: value.0,
            };

            state_writer.write_all(&entry.to_bytes())?;
            entry_offset += 1;
            has_entries = true;
        }

        if has_entries {
            stem_index_entries.push((stem, start_offset));
        }

        Ok(())
    })?;

    if entry_offset == 0 && head.stem_count > 0 {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            "Key index empty while state head indicates non-zero stems".to_string(),
        )));
    }

    state_writer.flush()?;
    let mut state_file = state_writer
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    state_file.seek(SeekFrom::Start(0))?;
    let final_header = StateHeader::new(entry_offset, head.block_number, chain_id, head.block_hash);
    state_file.write_all(&final_header.to_bytes())?;
    state_file.flush()?;

    let stem_index_file = File::create(&stem_index_path)?;
    let mut stem_writer = BufWriter::new(stem_index_file);

    let stem_count = stem_index_entries.len() as u64;
    stem_writer.write_all(&stem_count.to_le_bytes())?;

    for (stem, offset) in &stem_index_entries {
        stem_writer.write_all(stem.as_bytes())?;
        stem_writer.write_all(&offset.to_le_bytes())?;
    }
    stem_writer.flush()?;

    info!(
        entries = entry_offset,
        stems = stem_count,
        state_file = %state_path.display(),
        stem_index_file = %stem_index_path.display(),
        "PIR2 export complete (NOMT values)"
    );

    Ok(ExportResult {
        block_number: head.block_number,
        block_hash: head.block_hash,
        root: head.root,
        entry_count: entry_offset,
        stem_count,
        state_file: state_path.display().to_string(),
        stem_index_file: stem_index_path.display().to_string(),
    })
}

pub fn export_contract_state(
    db: &UbtDatabase,
    contract: Address,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    export_contract_state_from_mdbx(db, contract, output_dir, chain_id)
}

pub fn export_contract_state_from_mdbx(
    db: &UbtDatabase,
    contract: Address,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    let head = db.load_head()?.ok_or_else(|| {
        UbtError::Database(crate::error::DatabaseError::Mdbx(
            "No canonical state yet".to_string(),
        ))
    })?;

    info!(
        block = head.block_number,
        contract = %contract,
        "Exporting contract state to PIR2 format"
    );

    std::fs::create_dir_all(output_dir)?;

    let state_path = output_dir.join(format!("contract-{}.bin", contract));
    let stem_index_path = output_dir.join(format!("contract-{}-stem-index.bin", contract));

    let state_file = File::create(&state_path)?;
    let mut state_writer = BufWriter::new(state_file);

    let placeholder_header = StateHeader::new(0, head.block_number, chain_id, head.block_hash);
    state_writer.write_all(&placeholder_header.to_bytes())?;

    let mut entry_offset: u64 = 0;
    let mut stem_index_entries: Vec<(Stem, u64)> = Vec::new();
    let mut missing_addresses: Vec<Stem> = Vec::new();

    for (stem, stem_node) in db.iter_stems()? {
        let address = match db.load_stem_address(&stem)? {
            Some(addr) => addr,
            None => {
                missing_addresses.push(stem);
                continue;
            }
        };

        if address != contract {
            continue;
        }

        let mut subindices: Vec<_> = stem_node.values.keys().copied().collect();
        subindices.sort_unstable();

        if subindices.is_empty() {
            continue;
        }

        let start_offset = entry_offset;

        for subindex in subindices {
            let value = stem_node.values[&subindex];
            let tree_index = tree_index_from_key(&stem, subindex);

            let entry = StorageEntry {
                address: address.0 .0,
                tree_index,
                value: value.0,
            };

            state_writer.write_all(&entry.to_bytes())?;
            entry_offset += 1;
        }

        stem_index_entries.push((stem, start_offset));
    }

    if !missing_addresses.is_empty() {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "Missing stem->address mappings for {} stems. PIR export requires a fresh UBT build.",
                missing_addresses.len()
            ),
        )));
    }

    state_writer.flush()?;
    let mut state_file = state_writer
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    state_file.seek(SeekFrom::Start(0))?;
    let final_header = StateHeader::new(entry_offset, head.block_number, chain_id, head.block_hash);
    state_file.write_all(&final_header.to_bytes())?;
    state_file.flush()?;

    let stem_index_file = File::create(&stem_index_path)?;
    let mut stem_writer = BufWriter::new(stem_index_file);

    let stem_count = stem_index_entries.len() as u64;
    stem_writer.write_all(&stem_count.to_le_bytes())?;

    for (stem, offset) in &stem_index_entries {
        stem_writer.write_all(stem.as_bytes())?;
        stem_writer.write_all(&offset.to_le_bytes())?;
    }
    stem_writer.flush()?;

    info!(
        entries = entry_offset,
        stems = stem_count,
        contract = %contract,
        "Contract state export complete"
    );

    Ok(ExportResult {
        block_number: head.block_number,
        block_hash: head.block_hash,
        root: head.root,
        entry_count: entry_offset,
        stem_count,
        state_file: state_path.display().to_string(),
        stem_index_file: stem_index_path.display().to_string(),
    })
}

pub fn export_contract_state_from_nomt(
    _db: &UbtDatabase,
    nomt_dir: &Path,
    contract: Address,
    key_index_path: &Path,
    output_dir: &Path,
    chain_id: u64,
) -> Result<ExportResult> {
    let key_index = KeyIndex::open(key_index_path)?;
    let head = key_index.load_head()?.ok_or_else(|| {
        UbtError::Database(crate::error::DatabaseError::Mdbx(
            "Missing key index head metadata".to_string(),
        ))
    })?;

    info!(
        block = head.block_number,
        contract = %contract,
        "Exporting contract state to PIR2 format (NOMT values)"
    );

    std::fs::create_dir_all(output_dir)?;

    let state_path = output_dir.join(format!("contract-{}.bin", contract));
    let stem_index_path = output_dir.join(format!("contract-{}-stem-index.bin", contract));

    let state_file = File::create(&state_path)?;
    let mut state_writer = BufWriter::new(state_file);

    let placeholder_header = StateHeader::new(0, head.block_number, chain_id, head.block_hash);
    state_writer.write_all(&placeholder_header.to_bytes())?;

    let nomt = open_nomt(nomt_dir)?;

    let mut entry_offset: u64 = 0;
    let mut stem_index_entries: Vec<(Stem, u64)> = Vec::new();

    key_index.for_each_stem(|stem, record| {
        if record.address != contract {
            return Ok(());
        }

        let mut has_entries = false;
        let start_offset = entry_offset;

        for subindex in iter_bitmap_subindices(&record.bitmap) {
            let tree_index = tree_index_from_key(&stem, subindex);
            let value = read_nomt_value(&nomt, tree_index)?;

            let entry = StorageEntry {
                address: record.address.into_array(),
                tree_index,
                value: value.0,
            };

            state_writer.write_all(&entry.to_bytes())?;
            entry_offset += 1;
            has_entries = true;
        }

        if has_entries {
            stem_index_entries.push((stem, start_offset));
        }

        Ok(())
    })?;

    state_writer.flush()?;
    let mut state_file = state_writer
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    state_file.seek(SeekFrom::Start(0))?;
    let final_header = StateHeader::new(entry_offset, head.block_number, chain_id, head.block_hash);
    state_file.write_all(&final_header.to_bytes())?;
    state_file.flush()?;

    let stem_index_file = File::create(&stem_index_path)?;
    let mut stem_writer = BufWriter::new(stem_index_file);

    let stem_count = stem_index_entries.len() as u64;
    stem_writer.write_all(&stem_count.to_le_bytes())?;

    for (stem, offset) in &stem_index_entries {
        stem_writer.write_all(stem.as_bytes())?;
        stem_writer.write_all(&offset.to_le_bytes())?;
    }
    stem_writer.flush()?;

    info!(
        entries = entry_offset,
        stems = stem_count,
        contract = %contract,
        "Contract state export complete (NOMT values)"
    );

    Ok(ExportResult {
        block_number: head.block_number,
        block_hash: head.block_hash,
        root: head.root,
        entry_count: entry_offset,
        stem_count,
        state_file: state_path.display().to_string(),
        stem_index_file: stem_index_path.display().to_string(),
    })
}

#[derive(Debug, Clone)]
pub struct StateDeltaResult {
    pub from_block: u64,
    pub to_block: u64,
    pub head_block: u64,
    pub entry_count: u64,
    pub delta_file: String,
}

pub fn get_state_delta(
    db: &UbtDatabase,
    from_block: u64,
    to_block: u64,
    output_dir: &Path,
    chain_id: u64,
    delta_retention: u64,
) -> Result<StateDeltaResult> {
    let head = db.load_head()?.ok_or_else(|| {
        UbtError::Database(crate::error::DatabaseError::Mdbx(
            "No canonical state yet".to_string(),
        ))
    })?;

    if from_block > to_block {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "from_block {} is greater than to_block {}",
                from_block, to_block
            ),
        )));
    }

    if to_block > head.block_number {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "to_block {} is ahead of persisted head {}",
                to_block, head.block_number
            ),
        )));
    }

    let min_block = head.block_number.saturating_sub(delta_retention);
    if from_block < min_block {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "from_block {} is before delta retention window (min: {})",
                from_block, min_block
            ),
        )));
    }

    info!(
        from = from_block,
        to = to_block,
        head = head.block_number,
        "Computing state delta"
    );

    std::fs::create_dir_all(output_dir)?;

    use std::collections::HashSet;
    use ubt::TreeKey;

    let mut touched_keys_set: HashSet<TreeKey> = HashSet::new();
    let mut missing_addresses: Vec<Stem> = Vec::new();

    for block in from_block..=to_block {
        let deltas = db.load_block_deltas(block)?;
        for (stem, subindex, _old_value) in deltas {
            let key = TreeKey::new(stem, subindex);
            touched_keys_set.insert(key);
        }
    }

    let mut touched_keys: Vec<_> = touched_keys_set.into_iter().collect();
    touched_keys.sort_by(|a, b| {
        a.stem
            .as_bytes()
            .cmp(b.stem.as_bytes())
            .then(a.subindex.cmp(&b.subindex))
    });

    let delta_path = output_dir.join(format!("delta-{}-{}.bin", from_block, to_block));
    let delta_file = File::create(&delta_path)?;
    let mut delta_writer = BufWriter::new(delta_file);

    let placeholder_header = StateHeader::new(0, head.block_number, chain_id, head.block_hash);
    delta_writer.write_all(&placeholder_header.to_bytes())?;

    let mut entry_count: u64 = 0;

    for key in &touched_keys {
        let address = match db.load_stem_address(&key.stem)? {
            Some(addr) => addr,
            None => {
                missing_addresses.push(key.stem);
                continue;
            }
        };

        let value = db.load_value(key)?.unwrap_or(B256::ZERO);
        let tree_index = tree_index_from_key(&key.stem, key.subindex);

        let entry = StorageEntry {
            address: address.0 .0,
            tree_index,
            value: value.0,
        };

        delta_writer.write_all(&entry.to_bytes())?;
        entry_count += 1;
    }

    if !missing_addresses.is_empty() {
        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
            format!(
                "Missing stem->address mappings for {} stems. PIR export requires a fresh UBT build.",
                missing_addresses.len()
            ),
        )));
    }

    delta_writer.flush()?;
    let mut delta_file = delta_writer
        .into_inner()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    delta_file.seek(SeekFrom::Start(0))?;
    let final_header = StateHeader::new(entry_count, head.block_number, chain_id, head.block_hash);
    delta_file.write_all(&final_header.to_bytes())?;
    delta_file.flush()?;

    info!(
        from = from_block,
        to = to_block,
        entries = entry_count,
        "State delta export complete"
    );

    Ok(StateDeltaResult {
        from_block,
        to_block,
        head_block: head.block_number,
        entry_count,
        delta_file: delta_path.display().to_string(),
    })
}

fn tree_index_from_key(stem: &Stem, subindex: u8) -> [u8; 32] {
    let mut tree_index = [0u8; 32];
    tree_index[..31].copy_from_slice(stem.as_bytes());
    tree_index[31] = subindex;
    tree_index
}

fn iter_bitmap_subindices(bitmap: &[u8; 32]) -> impl Iterator<Item = u8> + '_ {
    (0u16..256).filter_map(move |idx| {
        let byte = bitmap[(idx / 8) as usize];
        let bit = (idx % 8) as u8;
        if (byte & (1u8 << bit)) != 0 {
            Some(idx as u8)
        } else {
            None
        }
    })
}

fn open_nomt(nomt_dir: &Path) -> Result<Nomt<NomtBlake3Hasher>> {
    let mut opts = NomtOptions::new();
    opts.path(nomt_dir);
    opts.rollback(true);
    opts.commit_concurrency(1);
    Nomt::open(opts)
        .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))
}

fn read_nomt_value(nomt: &Nomt<NomtBlake3Hasher>, tree_index: [u8; 32]) -> Result<B256> {
    let key: KeyPath = tree_index;
    let value = nomt
        .read(key)
        .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;
    match value {
        Some(bytes) => {
            let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                UbtError::Database(crate::error::DatabaseError::Mdbx(
                    "Invalid NOMT value length".to_string(),
                ))
            })?;
            Ok(B256::from(arr))
        }
        None => Ok(B256::ZERO),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_header_roundtrip() {
        let header = StateHeader::new(1000, 20_000_000, 1, B256::repeat_byte(0xab));
        let bytes = header.to_bytes();

        assert_eq!(&bytes[0..4], b"PIR2");
        assert_eq!(bytes.len(), STATE_HEADER_SIZE);
    }

    #[test]
    fn test_storage_entry_roundtrip() {
        let entry = StorageEntry {
            address: [0x42; 20],
            tree_index: [0x01; 32],
            value: [0xff; 32],
        };
        let bytes = entry.to_bytes();

        assert_eq!(bytes.len(), STATE_ENTRY_SIZE);
        assert_eq!(&bytes[0..20], &[0x42; 20]);
        assert_eq!(&bytes[20..52], &[0x01; 32]);
        assert_eq!(&bytes[52..84], &[0xff; 32]);
    }

    #[test]
    fn test_tree_index_from_key() {
        let stem = Stem::new([0xaa; 31]);
        let subindex = 42u8;

        let tree_index = tree_index_from_key(&stem, subindex);

        assert_eq!(&tree_index[..31], &[0xaa; 31]);
        assert_eq!(tree_index[31], 42);
    }

    #[test]
    fn test_inspire_core_format_compatibility() {
        let header = StateHeader::new(100, 20_000_000, 11155111, B256::repeat_byte(0xab));
        let bytes = header.to_bytes();

        assert_eq!(&bytes[0..4], b"PIR2", "Magic must be PIR2");
        assert_eq!(
            u16::from_le_bytes([bytes[4], bytes[5]]),
            1,
            "Version must be 1"
        );
        assert_eq!(
            u16::from_le_bytes([bytes[6], bytes[7]]),
            84,
            "Entry size must be 84"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            100,
            "Entry count"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            20_000_000,
            "Block number"
        );
        assert_eq!(
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            11155111,
            "Chain ID (Sepolia)"
        );
        assert_eq!(&bytes[32..64], &[0xab; 32], "Block hash");
    }

    #[test]
    fn test_storage_entry_inspire_core_layout() {
        let entry = StorageEntry {
            address: [0x42; 20],
            tree_index: [0x01; 32],
            value: [0xff; 32],
        };
        let bytes = entry.to_bytes();

        assert_eq!(&bytes[0..20], &[0x42; 20], "Address at offset 0");
        assert_eq!(&bytes[20..52], &[0x01; 32], "tree_index at offset 20");
        assert_eq!(&bytes[52..84], &[0xff; 32], "Value at offset 52");
    }

    #[test]
    fn test_stem_index_format() {
        let stem = Stem::new([0xbb; 31]);
        let offset: u64 = 12345;

        let mut entry_bytes = [0u8; 39];
        entry_bytes[0..31].copy_from_slice(stem.as_bytes());
        entry_bytes[31..39].copy_from_slice(&offset.to_le_bytes());

        assert_eq!(&entry_bytes[0..31], &[0xbb; 31], "Stem at offset 0");
        assert_eq!(
            u64::from_le_bytes(entry_bytes[31..39].try_into().unwrap()),
            12345,
            "Offset at byte 31"
        );
    }
}
