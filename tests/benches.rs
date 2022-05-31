use std::time::Instant;

use fast_kv_store::HashTable;
use rand::seq::SliceRandom;
use rand::Rng;
use tempdir::TempDir;

#[test]
fn ht_read_benchmark() {
    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);

    let num_elems = 10000;
    let num_iter = 100000;

    let mut keys = vec![];
    let mut values = vec![];
    let mut indexes = vec![];
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
        keys.push(key);
        values.push(value);

        let index = rand::thread_rng().gen_range(0..num_elems);
        indexes.push(index);
    }

    let start = Instant::now();
    for _ in 0..num_iter {
        let index = indexes.choose(&mut rand::thread_rng()).unwrap().clone();
        let key = &keys[index];
        let value = &values[index];
        assert_eq!(value, &db.get(key.clone()).unwrap());
    }
    let duration = start.elapsed();
    println!("read ns {}", duration.as_nanos() / num_iter);
}
