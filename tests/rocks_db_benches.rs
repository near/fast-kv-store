use std::time::Instant;

use fast_kv_store::HashTable;
use rand::{prelude::SliceRandom, Rng};
use rocksdb::DB;
use tempdir::TempDir;

fn genenrate_data(
    num_elems: usize,
    rdb: &DB,
    hdb: &mut HashTable,
) -> (Vec<(Vec<u8>, Vec<u8>)>, usize) {
    let mut data = vec![];
    let mut total_size = 0;
    for _ in 0..num_elems {
        let key: Vec<u8> = (0..rand::thread_rng().gen_range(6..8))
            .map(|_| rand::thread_rng().gen())
            .collect();
        total_size += key.len();
        let value: Vec<u8> = (0..rand::thread_rng().gen_range(0..1025))
            .map(|_| rand::thread_rng().gen())
            .collect();
        total_size += value.len();
        rdb.put(key.clone(), value.clone()).unwrap();
        assert_eq!(value, rdb.get(key.clone()).unwrap().unwrap());
        hdb.set(key.clone(), value.clone());
        assert_eq!(value, hdb.get(key.clone()).unwrap());
        data.push((key, value));
    }
    (data, total_size)
}

fn rdb_read(db: &DB, data: &[(Vec<u8>, Vec<u8>)]) -> u128 {
    let num_iter = 1_000_000;

    let start = Instant::now();
    for _ in 0..num_iter {
        let index = rand::thread_rng().gen_range(0..data.len());
        let (key, _value) = &data[index];
        db.get(key.clone()).unwrap().unwrap();
    }
    start.elapsed().as_nanos() / num_iter
}

fn ht_read(db: &mut HashTable, data: &[(Vec<u8>, Vec<u8>)]) -> u128 {
    let num_iter = 1_000_000;

    let start = Instant::now();
    for _ in 0..num_iter {
        let index = rand::thread_rng().gen_range(0..data.len());
        let (key, _value) = &data[index];
        db.get(key.clone()).unwrap();
    }
    start.elapsed().as_nanos() / num_iter
}

#[test]
fn benchmark_read() {
    println!();
    println!("elems\tMB\trocksdb\thashtable");
    for num_elems in [1_000, 10_000, 100_000, 1_000_000, 10_000_000] {
        let tmp_dir = TempDir::new("rdb").unwrap();
        let rocks_db = DB::open_default(tmp_dir.path().join("rdb")).unwrap();

        let tmp_dir = TempDir::new("hdb").unwrap();
        let salt = rand::thread_rng().gen::<[u8; 32]>();
        let mut hdb = HashTable::new(tmp_dir.path().join("hdb"), salt, None);

        let (data, total_size) = genenrate_data(num_elems, &rocks_db, &mut hdb);
        let rdb_elapsed = rdb_read(&rocks_db, &data);
        let hdb_elapsed = ht_read(&mut hdb, &data);
        println!(
            "{}\t{}\t{}\t{}",
            data.len(),
            total_size / 1024 / 1024,
            rdb_elapsed,
            hdb_elapsed
        );
    }
}

#[test]
fn rdb_benchmark_write() {
    let tmp_dir = TempDir::new("example").unwrap();
    let db = DB::open_default(tmp_dir.path().join("db")).unwrap();

    let num_elems = 10_000;
    let num_iter = 1_000_000;

    let mut keys = vec![];
    let mut values = vec![];
    for _ in 0..num_elems {
        let mut key = vec![];
        for _ in 0..rand::thread_rng().gen_range(6..8) {
            key.push(rand::thread_rng().gen());
        }
        let mut value = vec![];
        for _ in 0..rand::thread_rng().gen_range(0..1025) {
            value.push(rand::thread_rng().gen());
        }
        keys.push(key);
        values.push(value);
    }

    let mut indexes: Vec<usize> = (0..num_elems).collect();
    indexes.shuffle(&mut rand::thread_rng());

    let start = Instant::now();
    for i in 0..num_elems {
        let index = indexes[i];
        let key = &keys[index];
        let value = &values[index];
        db.put(key.clone(), value.clone()).unwrap();
    }
    let duration = start.elapsed();
    println!(
        "\nRocksDB first write {}ns",
        duration.as_nanos() / num_elems as u128
    );

    for i in 0..num_elems {
        let key = &keys[i];
        let value = &values[i];
        assert_eq!(value, &db.get(key.clone()).unwrap().unwrap());
    }

    let indexes: Vec<usize> = (0..num_elems).collect();
    let start = Instant::now();
    for _ in 0..num_iter {
        let index = indexes.choose(&mut rand::thread_rng()).unwrap().clone();
        let key = &keys[index];
        let value = &values[index];
        db.put(key.clone(), value.clone()).unwrap();
    }
    let duration = start.elapsed();
    println!("\nRocksDB over write {}ns", duration.as_nanos() / num_iter);
}
