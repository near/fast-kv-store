use criterion::{criterion_group, criterion_main, Criterion};
use fast_kv_store::HashTable;
use rand::Rng;
use tempdir::TempDir;

mod rocksdb;

fn ht_read_benchmark(c: &mut Criterion) {
    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);
    let mut key = vec![];
    for _ in 0..rand::thread_rng().gen_range(6..8) {
        key.push(rand::thread_rng().gen());
    }
    let mut value = vec![];
    for _ in 0..rand::thread_rng().gen_range(0..1025) {
        value.push(rand::thread_rng().gen());
    }
    c.bench_function("HashTable read", |b| {
        b.iter(|| db.set(key.clone(), value.clone()))
    });
}

fn rdb_read_benchmark(c: &mut Criterion) {
    let db_config = rocksdb::RocksDBTestConfig::default();
    c.bench_function("RocksDb read", |b| {
        b.iter(|| rocksdb::rocks_db_read_cost(&db_config))
    });
}

criterion_group!(benches, ht_read_benchmark, rdb_read_benchmark);
criterion_main!(benches);