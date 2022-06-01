use std::{cmp, path::Path, time::Instant};

use fast_kv_store::HashTable;
use rand::{prelude::SliceRandom, Rng};
use rocksdb::{Options, DB};
use tempdir::TempDir;

const NUM_ITER: u128 = 1_000_000;

fn genenrate_data(
    num_elems: usize,
    default_rdb: &DB,
    settings_rdb: &DB,
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
        default_rdb.put(key.clone(), value.clone()).unwrap();
        assert_eq!(value, default_rdb.get(key.clone()).unwrap().unwrap());
        settings_rdb.put(key.clone(), value.clone()).unwrap();
        assert_eq!(value, settings_rdb.get(key.clone()).unwrap().unwrap());
        hdb.set(key.clone(), value.clone());
        assert_eq!(value, hdb.get(key.clone()).unwrap());
        data.push((key, value));
    }
    (data, total_size)
}

fn rdb_read(db: &DB, data: &[(Vec<u8>, Vec<u8>)]) -> u128 {
    let start = Instant::now();
    for _ in 0..NUM_ITER {
        let index = rand::thread_rng().gen_range(0..data.len());
        let (key, _value) = &data[index];
        db.get(key.clone()).unwrap().unwrap();
    }
    start.elapsed().as_nanos() / NUM_ITER
}

fn ht_read(db: &mut HashTable, data: &[(Vec<u8>, Vec<u8>)]) -> u128 {
    let start = Instant::now();
    for _ in 0..NUM_ITER {
        let index = rand::thread_rng().gen_range(0..data.len());
        let (key, _value) = &data[index];
        db.get(key.clone()).unwrap();
    }
    start.elapsed().as_nanos() / NUM_ITER
}

fn set_compression_options(opts: &mut Options) {
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    // RocksDB documenation says that 16KB is a typical dictionary size.
    // We've empirically tuned the dicionary size to twice of that 'typical' size.
    // Having train data size x100 from dictionary size is a recommendation from RocksDB.
    // See: https://rocksdb.org/blog/2021/05/31/dictionary-compression.html?utm_source=dbplatz
    let dict_size = 2 * 16384;
    let max_train_bytes = dict_size * 100;
    // We use default parameters of RocksDB here:
    //      window_bits is -14 and is unused (Zlib-specific parameter),
    //      compression_level is 32767 meaning the default compression level for ZSTD,
    //      compression_strategy is 0 and is unused (Zlib-specific parameter).
    // See: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/advanced_options.h#L176:
    opts.set_bottommost_compression_options(
        /*window_bits */ -14, /*compression_level */ 32767,
        /*compression_strategy */ 0, dict_size, /*enabled */ true,
    );
    opts.set_bottommost_zstd_max_train_bytes(max_train_bytes, true);
}

fn rocksdb_options() -> Options {
    let mut opts = Options::default();

    set_compression_options(&mut opts);
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    opts.set_use_fsync(false);
    opts.set_max_open_files(i32::MAX);
    opts.set_keep_log_file_num(1);
    opts.set_bytes_per_sync(bytesize::MIB);
    opts.set_write_buffer_size(256 * bytesize::MIB as usize);
    opts.set_max_bytes_for_level_base(256 * bytesize::MIB);
    if cfg!(feature = "single_thread_rocksdb") {
        opts.set_disable_auto_compactions(true);
        opts.set_max_background_jobs(0);
        opts.set_stats_dump_period_sec(0);
        opts.set_stats_persist_period_sec(0);
        opts.set_level_zero_slowdown_writes_trigger(-1);
        opts.set_level_zero_file_num_compaction_trigger(-1);
        opts.set_level_zero_stop_writes_trigger(100000000);
    } else {
        opts.increase_parallelism(cmp::max(1, num_cpus::get() as i32 / 2));
        opts.set_max_total_wal_size(bytesize::GIB);
    }

    opts
}

#[test]
fn benchmark_read() {
    println!();
    println!("elems\tSize\tDefaultRDB\tSettingsRDB\thashtable");
    //for num_elems in [1_000, 10_000, 100_000, 1_000_000] {
    for num_elems in [5_000_000] {
        let exp_path = format!("{}/experiments", std::env::var("HOME").unwrap());
        let exp_dir = Path::new(&exp_path);

        let default_rdb = {
            let path = exp_dir.join("default-rdb");
            DB::open_default(path).unwrap()
        };

        let settings_rdb = {
            let options = rocksdb_options();
            let path = exp_dir.join("settings-rdb");
            DB::open(&options, path).unwrap()
        };

        let mut hdb = {
            let salt = rand::thread_rng().gen::<[u8; 32]>();
            let path = exp_dir.join("hdb");
            HashTable::new(path, salt, None)
        };

        let (data, total_size) = genenrate_data(num_elems, &default_rdb, &settings_rdb, &mut hdb);
        let default_rdb_elapsed = rdb_read(&default_rdb, &data);
        let settings_rdb_elapsed = rdb_read(&settings_rdb, &data);
        let hdb_elapsed = ht_read(&mut hdb, &data);
        println!(
            "{}\t{}\t{}\t{}\t{}",
            data.len(),
            bytesize::to_string(total_size as u64, true),
            default_rdb_elapsed,
            settings_rdb_elapsed,
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
