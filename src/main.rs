use std::{path::Path, time::Instant};

use clap::Parser;
use fast_kv_store::HashTable;
use near_store::StoreConfig;
use rand::Rng;
use rocksdb::{IteratorMode, DB};

const NUM_ELEMS: usize = 1_000_000;
const NUM_ITER: u128 = 1_000_000;

fn new_rocks_db_with_default_settings(exp_dir: &Path) -> DB {
    let path = exp_dir.join("default-rdb");
    println!(
        "Creating default rocksdb at {}",
        path.as_os_str().to_str().unwrap()
    );

    DB::open_default(path).unwrap()
}

fn new_rocks_db_with_near_settings(exp_dir: &Path) -> DB {
    let path = exp_dir.join("settings-rdb");
    let (db, _) = near_store::RocksDB::open_read_write(&path, &StoreConfig::default()).unwrap();
    println!(
        "Creating setting rocksdb at {}",
        path.as_os_str().to_str().unwrap()
    );
    db
}

fn new_hash_table(exp_dir: &Path) -> HashTable {
    // Hardcoding a randomly generated salt.
    let salt = [
        228, 201, 35, 78, 45, 173, 2, 58, 141, 250, 210, 214, 48, 142, 202, 190, 28, 93, 106, 106,
        125, 76, 93, 13, 80, 34, 177, 143, 138, 138, 4, 39,
    ];
    let path = exp_dir.join("hdb");
    println!(
        "Creating hash table at {}",
        path.as_os_str().to_str().unwrap()
    );
    HashTable::new(path, salt, None)
}

fn genenrate_data(exp_dir: &Path) {
    let default_rdb = new_rocks_db_with_default_settings(exp_dir);
    let settings_rdb = new_rocks_db_with_near_settings(exp_dir);
    let mut hdb = new_hash_table(exp_dir);

    let start = Instant::now();
    for _ in 0..NUM_ELEMS {
        let key: Vec<u8> = (0..rand::thread_rng().gen_range(6..8))
            .map(|_| rand::thread_rng().gen())
            .collect();
        let value: Vec<u8> = (0..rand::thread_rng().gen_range(0..1025))
            .map(|_| rand::thread_rng().gen())
            .collect();
        default_rdb.put(key.clone(), value.clone()).unwrap();
        assert_eq!(value, default_rdb.get(key.clone()).unwrap().unwrap());
        settings_rdb.put(key.clone(), value.clone()).unwrap();
        assert_eq!(value, settings_rdb.get(key.clone()).unwrap().unwrap());
        hdb.set(key.clone(), value.clone());
        assert_eq!(value, hdb.get(key.clone()).unwrap());
    }
    let elapsed = start.elapsed().as_nanos() / NUM_ELEMS as u128;
    println!("Generated data in {}", elapsed);
    hdb.flush_changes();
}

fn rdb_read(db: &DB, data: &[Vec<u8>]) -> u128 {
    let start = Instant::now();
    for _ in 0..NUM_ITER {
        let index = rand::thread_rng().gen_range(0..data.len());
        db.get(data[index].clone()).unwrap().unwrap();
    }
    start.elapsed().as_nanos() / NUM_ITER
}

fn ht_read(db: &mut HashTable, data: &[Vec<u8>]) -> u128 {
    let start = Instant::now();
    for _ in 0..NUM_ITER {
        let index = rand::thread_rng().gen_range(0..data.len());
        db.get(data[index].clone()).unwrap();
    }
    start.elapsed().as_nanos() / NUM_ITER
}

fn read_data(default_rdb: &DB, setting_rdb: &DB, hdb: &mut HashTable) -> (Vec<Vec<u8>>, usize) {
    let mut data = vec![];
    let mut total_size = 0;
    for (key, value) in default_rdb.iterator(IteratorMode::Start) {
        let key = key.to_vec();
        let value = value.to_vec();
        assert_eq!(setting_rdb.get(key.clone()).unwrap().unwrap(), value);
        assert_eq!(hdb.get(key.clone()).unwrap(), value);
        total_size += key.len() + value.len();
        data.push(key);
    }
    (data, total_size)
}

fn run_experiments(exp_dir: &Path) {
    let default_rdb = new_rocks_db_with_default_settings(exp_dir);
    let settings_rdb = new_rocks_db_with_near_settings(exp_dir);
    let mut hdb = new_hash_table(exp_dir);
    let (data, total_size) = read_data(&default_rdb, &settings_rdb, &mut hdb);
    let default_rdb_elapsed = rdb_read(&default_rdb, &data);
    let settings_rdb_elapsed = rdb_read(&settings_rdb, &data);
    let hdb_elapsed = ht_read(&mut hdb, &data);
    println!("elems\tSize\tDefaultRDB\tSettingsRDB\thashtable");
    println!(
        "{}\t{}\t{}\t{}\t{}",
        data.len(),
        bytesize::to_string(total_size as u64, true),
        default_rdb_elapsed,
        settings_rdb_elapsed,
        hdb_elapsed
    );
}

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    run_experiments: bool,
}

fn main() {
    let exp_path = format!("{}/experiments", std::env::var("HOME").unwrap());
    let exp_dir = Path::new(&exp_path);
    let args = Args::parse();
    if args.run_experiments {
        println!("Running experiments");
        run_experiments(exp_dir);
    } else {
        println!("Generating data");
        genenrate_data(exp_dir);
    }
}
