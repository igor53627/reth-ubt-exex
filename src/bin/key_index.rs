//! Inspect the UBT key index (redb) used for NOMT exports.

use std::path::PathBuf;

use clap::Parser;
use eyre::Result;
use hex;
use ubt_exex::key_index::{KeyIndex, KEY_INDEX_FILE};

#[derive(Parser, Debug)]
#[command(about = "Inspect the UBT key index database")]
struct Args {
    /// Base data directory (defaults to RETH_DATA_DIR or current directory)
    #[arg(long = "data-dir")]
    data_dir: Option<PathBuf>,

    /// Count stems by iterating the key index
    #[arg(long)]
    count_stems: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let data_dir = resolve_data_dir(args.data_dir);
    let key_index_path = data_dir.join(KEY_INDEX_FILE);

    let key_index = KeyIndex::open(&key_index_path)?;

    println!("Key index: {}", key_index_path.display());

    match key_index.load_head()? {
        Some(head) => {
            println!("Head block: {}", head.block_number);
            println!("Block hash: 0x{}", hex::encode(head.block_hash.0));
            println!("Root: 0x{}", hex::encode(head.root.0));
            println!("Stem count (metadata): {}", head.stem_count);

            if args.count_stems {
                let mut count: u64 = 0;
                key_index.for_each_stem(|_, _| {
                    count += 1;
                    Ok(())
                })?;
                println!("Stem count (iterated): {}", count);
                if count != head.stem_count {
                    println!("Warning: stem count mismatch");
                }
            }
        }
        None => {
            println!("No head metadata found");
        }
    }

    Ok(())
}

fn resolve_data_dir(arg: Option<PathBuf>) -> PathBuf {
    if let Some(path) = arg {
        return path;
    }
    if let Ok(path) = std::env::var("RETH_DATA_DIR") {
        return PathBuf::from(path);
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
