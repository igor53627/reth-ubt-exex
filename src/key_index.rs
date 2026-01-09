//! Key index for NOMT-only exports.
//!
//! Stores per-stem address + subindex bitmap so we can enumerate keys without MDBX.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256};
use redb::{Database, ReadableTable, TableDefinition, TableError};
use ubt::Stem;

use crate::error::{Result, UbtError};

pub const KEY_INDEX_FILE: &str = "key-index.redb";
const STEM_TABLE: TableDefinition<&[u8; 31], &[u8; 52]> = TableDefinition::new("ubt_stems");
const META_TABLE: TableDefinition<&str, &[u8; 80]> = TableDefinition::new("ubt_meta");
const META_HEAD_KEY: &str = "head";

#[derive(Debug, Clone)]
pub struct StemRecord {
    pub address: Address,
    pub bitmap: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct HeadRecord {
    pub block_number: u64,
    pub block_hash: B256,
    pub root: B256,
    pub stem_count: u64,
}

pub struct KeyIndex {
    db: Database,
    path: PathBuf,
}

impl KeyIndex {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = if path.exists() {
            Database::open(&path)
        } else {
            Database::create(&path)
        }
        .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        Ok(Self { db, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    /// Update the index for a batch of (stem, subindex, address) entries.
    /// Returns the number of new stems added.
    pub fn apply_updates(
        &self,
        updates: impl IntoIterator<Item = (Stem, u8, Address)>,
    ) -> Result<usize> {
        let mut per_stem: HashMap<Stem, (Address, [u8; 32])> = HashMap::new();
        for (stem, subindex, address) in updates {
            let entry = per_stem.entry(stem).or_insert_with(|| (address, [0u8; 32]));
            if entry.0 != address {
                return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
                    "Stem address mismatch in key index".to_string(),
                )));
            }
            set_bit(&mut entry.1, subindex);
        }

        if per_stem.is_empty() {
            return Ok(0);
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        let new_stems = {
            let mut table = write_txn
                .open_table(STEM_TABLE)
                .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

            let mut new_stems = 0usize;

            for (stem, (address, bitmap)) in per_stem {
                let key = stem.as_bytes();
                let mut merged_bitmap = bitmap;

                if let Some(existing) = table
                    .get(key)
                    .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?
                {
                    let existing_bytes = existing.value();
                    let (existing_addr, existing_bitmap) = split_value(existing_bytes)?;
                    if existing_addr != address {
                        return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
                            "Stem address mismatch in key index".to_string(),
                        )));
                    }
                    for i in 0..32 {
                        merged_bitmap[i] |= existing_bitmap[i];
                    }
                } else {
                    new_stems += 1;
                }

                let value = pack_value(address, merged_bitmap);
                table
                    .insert(key, &value)
                    .map_err(|e| {
                        UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string()))
                    })?;
            }

            new_stems
        };

        write_txn
            .commit()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        Ok(new_stems)
    }

    pub fn save_head(
        &self,
        block_number: u64,
        block_hash: B256,
        root: B256,
        stem_count: u64,
    ) -> Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        {
            let mut table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;
            let value = pack_head(block_number, block_hash, root, stem_count);
            table
                .insert(META_HEAD_KEY, &value)
                .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;
        }

        write_txn
            .commit()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        Ok(())
    }

    pub fn load_head(&self) -> Result<Option<HeadRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        let table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => {
                return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
                    e.to_string(),
                )))
            }
        };

        let value = match table.get(META_HEAD_KEY) {
            Ok(Some(value)) => value,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
                    e.to_string(),
                )))
            }
        };

        Ok(Some(unpack_head(value.value())?))
    }

    /// Iterate all stem records in sorted order, invoking the provided callback.
    pub fn for_each_stem<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Stem, StemRecord) -> Result<()>,
    {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;
        let table = match read_txn.open_table(STEM_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(e) => {
                return Err(UbtError::Database(crate::error::DatabaseError::Mdbx(
                    e.to_string(),
                )))
            }
        };
        let iter = table
            .iter()
            .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;

        for entry in iter {
            let (key, value) = entry
                .map_err(|e| UbtError::Database(crate::error::DatabaseError::Mdbx(e.to_string())))?;
            let stem = Stem::new(*key.value());
            let (address, bitmap) = split_value(value.value())?;
            f(stem, StemRecord { address, bitmap })?;
        }

        Ok(())
    }
}

fn set_bit(bitmap: &mut [u8; 32], subindex: u8) {
    let byte = (subindex / 8) as usize;
    let bit = subindex % 8;
    bitmap[byte] |= 1u8 << bit;
}

fn pack_value(address: Address, bitmap: [u8; 32]) -> [u8; 52] {
    let mut value = [0u8; 52];
    let addr_bytes: [u8; 20] = address.into_array();
    value[..20].copy_from_slice(&addr_bytes);
    value[20..].copy_from_slice(&bitmap);
    value
}

fn split_value(bytes: &[u8; 52]) -> Result<(Address, [u8; 32])> {
    let mut addr_bytes = [0u8; 20];
    let mut bitmap = [0u8; 32];
    addr_bytes.copy_from_slice(&bytes[..20]);
    bitmap.copy_from_slice(&bytes[20..]);
    Ok((Address::from(addr_bytes), bitmap))
}

fn pack_head(
    block_number: u64,
    block_hash: B256,
    root: B256,
    stem_count: u64,
) -> [u8; 80] {
    let mut value = [0u8; 80];
    value[..8].copy_from_slice(&block_number.to_le_bytes());
    value[8..40].copy_from_slice(&block_hash.0);
    value[40..72].copy_from_slice(&root.0);
    value[72..80].copy_from_slice(&stem_count.to_le_bytes());
    value
}

fn unpack_head(bytes: &[u8; 80]) -> Result<HeadRecord> {
    let mut block_number_bytes = [0u8; 8];
    let mut block_hash = [0u8; 32];
    let mut root = [0u8; 32];
    let mut stem_count_bytes = [0u8; 8];

    block_number_bytes.copy_from_slice(&bytes[..8]);
    block_hash.copy_from_slice(&bytes[8..40]);
    root.copy_from_slice(&bytes[40..72]);
    stem_count_bytes.copy_from_slice(&bytes[72..80]);

    Ok(HeadRecord {
        block_number: u64::from_le_bytes(block_number_bytes),
        block_hash: B256::from(block_hash),
        root: B256::from(root),
        stem_count: u64::from_le_bytes(stem_count_bytes),
    })
}
