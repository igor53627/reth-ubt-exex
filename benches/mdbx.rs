use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use tempfile::TempDir;

use alloy_primitives::B256;
use ubt::{Stem, StemNode, TreeKey};
use ubt_exex::persistence::UbtDatabase;

fn stem_from_u64(i: u64) -> Stem {
    let mut bytes = [0u8; 31];
    bytes[23..31].copy_from_slice(&i.to_be_bytes());
    Stem::new(bytes)
}

fn value_from_u64(i: u64, subindex: u8) -> B256 {
    let mut bytes = [0u8; 32];
    let v = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ subindex as u64;
    bytes[24..32].copy_from_slice(&v.to_be_bytes());
    B256::from(bytes)
}

fn make_updates(stem_count: usize, values_per_stem: u8) -> Vec<(Stem, StemNode)> {
    let mut updates = Vec::with_capacity(stem_count);
    for i in 0..stem_count as u64 {
        let stem = stem_from_u64(i);
        let mut node = StemNode::new(stem);
        for sub in 0..values_per_stem {
            node.set_value(sub, value_from_u64(i, sub));
        }
        updates.push((stem, node));
    }
    updates
}

fn make_keys(updates: &[(Stem, StemNode)], values_per_stem: u8) -> Vec<TreeKey> {
    let mut keys = Vec::new();
    for (stem, _) in updates {
        for sub in 0..values_per_stem {
            keys.push(TreeKey::new(*stem, sub));
        }
    }
    keys
}

fn bench_batch_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("mdbx_batch_update");
    let cases = [1_000usize, 10_000usize];
    let values_per_stem = 8u8;

    for stem_count in cases {
        let updates = make_updates(stem_count, values_per_stem);
        group.throughput(criterion::Throughput::Elements(stem_count as u64));
        group.bench_with_input(
            BenchmarkId::new("stems", stem_count),
            &updates,
            |b, updates| {
                b.iter_batched(
                    || {
                        let dir = TempDir::new().expect("tempdir");
                        let db = UbtDatabase::open(dir.path()).expect("db open");
                        (db, dir)
                    },
                    |(db, _dir)| {
                        db.batch_update_stems(updates).expect("batch update");
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_load_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("mdbx_load_value");
    let stem_count = 10_000usize;
    let values_per_stem = 8u8;
    let updates = make_updates(stem_count, values_per_stem);
    let keys = make_keys(&updates, values_per_stem);

    let dir = TempDir::new().expect("tempdir");
    let db = UbtDatabase::open(dir.path()).expect("db open");
    db.batch_update_stems(&updates).expect("batch update");

    group.bench_function("load_value", |b| {
        b.iter(|| {
            for key in &keys {
                black_box(db.load_value(key).expect("load value"));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_batch_update, bench_load_value);
criterion_main!(benches);
