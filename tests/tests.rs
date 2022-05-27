use rand::seq::SliceRandom;
use rand::Rng;
use fast_kv_store::HashTable;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::time::Instant;
use tempdir::TempDir;

fn open_file(path: &Path, truncate: bool) -> File {
    if truncate {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("")
    } else {
        OpenOptions::new().read(true).open(path).expect("")
    }
}

#[test]
fn test_fuzzy_db_ht_consistency() {
    // During half inserts, half gets the resize happens around 29K.
    // In the non-fuzzy mode we do 35K inserts, then do 10K more iterations
    // with deletes.
    #[cfg(not(feature = "long_fuzz"))]
    const NUM_ITERS: usize = 45000;
    #[cfg(all(feature = "long_fuzz", debug_assertions))]
    const NUM_ITERS: usize = 250000;
    #[cfg(all(feature = "long_fuzz", not(debug_assertions)))]
    const NUM_ITERS: usize = 5000000000;

    const REWRITES_START: usize = 20000;
    const DELETES_START: usize = 35000;
    const COMMIT_EVERY: usize = 2501;

    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);

    let mut map: HashMap<Vec<u8>, u64> = HashMap::new();
    let mut all_keys = vec![];

    let mut started = Instant::now();
    let mut inserts: usize = 0;
    let mut deletes: usize = 0;
    let mut reads: usize = 0;

    let mut odd: usize = 0;

    for iter in 0..NUM_ITERS {
        let want_delete =
            iter > DELETES_START && rand::thread_rng().gen() && rand::thread_rng().gen();

        let key = if iter > REWRITES_START && all_keys.len() > 0 && want_delete {
            all_keys.choose(&mut rand::thread_rng()).cloned().unwrap()
        } else {
            let v = rand::thread_rng().gen::<[u8; 32]>().to_vec();
            all_keys.push(v.clone());
            v
        };
        let value = rand::thread_rng().gen_range(1..1u64 << 48);

        let adjusted_key = [vec![32], key.clone()].concat();

        if odd <= 1 {
            assert_eq!(db.ht_get(adjusted_key.clone()), map.get(&key).cloned());
        }
        if odd != 0 {
            if want_delete {
                map.remove(&key);
                db.ht_delete(adjusted_key.clone());
                deletes += 1;
            } else {
                map.insert(key.clone(), value.clone());
                db.ht_set(adjusted_key.clone(), value);
                inserts += 1;
            }
        }
        if odd <= 1 {
            assert_eq!(db.ht_get(adjusted_key.clone()), map.get(&key).cloned());
            reads += 1;
        }

        if (iter + 1) % COMMIT_EVERY == 0 {
            db.flush_changes();
        }

        if (iter + 1) % 1000000 == 0 {
            println!(
                "{} reads, {} writes and {} deletes in {}",
                reads,
                inserts,
                deletes,
                started.elapsed().as_millis()
            );
            started = Instant::now();
            reads = 0;
            inserts = 0;
            deletes = 0;
            odd = (odd + 1) % 5;
        }
    }

    println!(
        "{} reads, {} writes and {} deletes in {}",
        reads,
        inserts,
        deletes,
        started.elapsed().as_millis()
    );

    tmp_dir.close().unwrap();
}

#[test]
fn test_fuzzy_storage_consistency() {
    #[cfg(not(feature = "long_fuzz"))]
    const NUM_ITERS: usize = 5000;
    #[cfg(all(feature = "long_fuzz", debug_assertions))]
    const NUM_ITERS: usize = 100000;
    #[cfg(all(feature = "long_fuzz", not(debug_assertions)))]
    const NUM_ITERS: usize = 10000000000;

    const REWRITES_START: usize = 5000;
    const COMMIT_EVERY: usize = 2501;
    // Must be a multiple of `COMMIT_EVERY`
    const RECREATE_EVERY: usize = COMMIT_EVERY * 212;

    let tmp_dir = TempDir::new("example").unwrap();
    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let mut db = HashTable::new(tmp_dir.path().join("db"), salt, None);

    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut all_keys = vec![];

    let mut started = Instant::now();
    let mut inserts: usize = 0;
    let mut deletes: usize = 0;
    let mut reads: usize = 0;

    let mut odd: u32 = 0;
    for iter in 0..NUM_ITERS {
        let want_delete = odd != 3
            && (odd == 4 || rand::thread_rng().gen() && (odd == 5 || rand::thread_rng().gen()));

        let read_iter = odd <= 1 || odd >= 8;
        let key = if all_keys.len() > 0
            && (iter > REWRITES_START && rand::thread_rng().gen() && rand::thread_rng().gen()
                || want_delete
                || read_iter)
        {
            all_keys.choose(&mut rand::thread_rng()).cloned().unwrap()
        } else {
            let mut v = vec![];
            for _ in 0..rand::thread_rng().gen_range(6..8) {
                v.push(rand::thread_rng().gen());
            }
            all_keys.push(v.clone());
            v
        };

        let mut value = vec![];
        for _ in 0..rand::thread_rng().gen_range(0..1025) {
            value.push(rand::thread_rng().gen());
        }

        if odd != 0 {
            if want_delete {
                map.remove(&key);
                db.delete(key.clone());
                deletes += 1;
            } else {
                map.insert(key.clone(), value.clone());
                db.set(key.clone(), value);
                inserts += 1;
            }
        }
        if read_iter {
            assert_eq!(db.get(key.clone()), map.get(&key).cloned());
            reads += 1;
        }

        if (iter + 1) % RECREATE_EVERY == 0 {
            println!("WRITING TO WAL ...");
            db.write_to_log(&mut open_file(&tmp_dir.path().join("wal"), true));
            println!("RECREATING ...");
            db = HashTable::new(
                tmp_dir.path().join("db"),
                salt,
                Some(&mut open_file(&tmp_dir.path().join("wal"), false)),
            );
            println!("DONE");
        } else if (iter + 1) % COMMIT_EVERY == 0 {
            db.flush_changes();
        }

        if (iter + 1) % 100000 == 0 {
            println!(
                "{} reads, {} writes and {} deletes in {}ms",
                reads,
                inserts,
                deletes,
                started.elapsed().as_millis(),
            );
            //db.print_stats();
            db.reset_del_balance();
            started = Instant::now();
            reads = 0;
            inserts = 0;
            deletes = 0;
            odd = (odd + 1) % 10;
        }
    }

    println!(
        "{} reads, {} writes and {} deletes in {}ms",
        reads,
        inserts,
        deletes,
        started.elapsed().as_millis()
    );

    tmp_dir.close().unwrap();
}

/*#[test]
fn test_sanity_storage() {
    let tmp_dir = TempDir::new("example").unwrap();
    let db = HashTable::new(
        tmp_dir.path().join("db"),
        rand::thread_rng().gen::<[u8; 32]>(),
    );

    let mut table = Table::new(db);

    // Initially we expect the next PKs to be zero
    assert_eq!(table.get_next_pk(), 0);

    // Insert a small key-value pair, a larger key-value pair (that is expected to span 4 slots),
    // and then rewrite the small key value pair
    for (k, v, old_v) in vec![(b"foobar".to_vec(), b"baz".to_vec(), None), (b"second".to_vec(), b"some potentially long-ish value that won't fit into a single value slot, pretty long ain't it. A little bit more. Even more. Let's stress it! May be some generative way of creating this constant was a better idea?".to_vec(), None), (b"foobar".to_vec(), b"new value".to_vec(), Some(b"baz".to_vec()))] {
        assert_eq!(table.get(k.clone()), old_v.clone());
        table.set(k.clone(), v.clone());
        assert_eq!(table.get(k.clone()), Some(v.clone()));
    }

    // The table must have reused the ID on the rewrite, so we expect it to still have next PK
    // equal to 5
    assert_eq!(table.get_next_pk(), 5);

    // Delete all the inserted data. This should not affect the next PKs, but should populate the
    // free list
    for k in vec![b"foobar".to_vec(), b"second".to_vec()] {
        table.delete(k.clone());
        assert_eq!(table.get(k.clone()), None);
    }

    assert_eq!(table.get_next_pk(), 5);

    // Insert 128 small key-value pairs. We expect that the state table will reuse the first 5
    // slots, and then start using new PKs. The metadata table will be allocating new PKs from the
    // get go.
    for i in 0u64..128 {
        let x = i.to_le_bytes().to_vec();

        assert_eq!(table.get(x.clone()), None);

        table.set(x.clone(), x.clone());

        assert_eq!(table.get_next_pk(), 5 + i.saturating_sub(4));
    }

    assert_eq!(table.get_next_pk(), 128);

    // Now insert longer values (that occupy three slots) into the same keys. Expect the table
    // to grow PKs at 2 per insert (reusing the existing one)
    for i in 0u64..128 {
        let x = i.to_le_bytes().to_vec();

        assert_eq!(table.get(x.clone()), Some(x.clone()));

        table.set(x.clone(), [x[0]; 140].to_vec());

        assert_eq!(table.get(x.clone()), Some(vec![x[0]; 140]));

        assert_eq!(table.get_next_pk(), 128 + (i + 1) * 2);
    }

    assert_eq!(table.get_next_pk(), 384);

    // Now insert even longer values (that occupy five slots) into the same keys. Expect the table
    // to grow PKs at 2 per insert (reusing the existing ones)
    for i in 0u64..128 {
        let x = i.to_le_bytes().to_vec();
        assert_eq!(table.get(x.clone()), Some(vec![x[0]; 140]));

        table.set(x.clone(), [1 + x[0]; 300].to_vec());

        assert_eq!(table.get(x.clone()), Some(vec![1 + x[0]; 300]));

        assert_eq!(table.get_next_pk(), 384 + (i + 1) * 2);
    }
}*/
