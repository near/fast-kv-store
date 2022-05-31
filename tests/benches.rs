use std::time::Instant;

use fast_kv_store::HashTable;
use rand::seq::SliceRandom;
use rand::Rng;
use tempdir::TempDir;

#[test]
fn ht_benchmark_read() {
    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);

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
        db.set(key.clone(), value.clone());
        assert_eq!(value, db.get(key.clone()).unwrap());
        keys.push(key);
        values.push(value);
    }

    let indexes: Vec<usize> = (0..num_elems).collect();
    let start = Instant::now();
    for _ in 0..num_iter {
        let index = indexes.choose(&mut rand::thread_rng()).unwrap().clone();
        let key = &keys[index];
        db.get(key.clone()).unwrap();
    }
    let duration = start.elapsed();
    println!("\nHashTable read {}ns", duration.as_nanos() / num_iter);
}

#[test]
fn ht_benchmark_write() {
    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);

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
        db.set(key.clone(), value.clone());
    }
    let duration = start.elapsed();
    println!(
        "\nHashTable first write {}ns",
        duration.as_nanos() / num_elems as u128
    );

    for i in 0..num_elems {
        let key = &keys[i];
        let value = &values[i];
        assert_eq!(value, &db.get(key.clone()).unwrap());
    }

    let indexes: Vec<usize> = (0..num_elems).collect();
    let start = Instant::now();
    for _ in 0..num_iter {
        let index = indexes.choose(&mut rand::thread_rng()).unwrap().clone();
        let key = &keys[index];
        let value = &values[index];
        db.set(key.clone(), value.clone());
    }
    let duration = start.elapsed();
    println!(
        "\nHashTable over write {}ns",
        duration.as_nanos() / num_iter
    );
}
