use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::thread;

use blake3;

const PAGE_TYPE_FREE: u64 = 0;
const PAGE_TYPE_HT: u64 = 1;
const PAGE_TYPE_VALUES: u64 = 2;
const PAGE_TYPE_DELMAP: u64 = 3;

const NUM_FLUSH_THREADS: usize = 1;
const PAGE_SIZE: u64 = 4 * 1024;
const SLOT_SIZE: u64 = 32;
const VALUE_SIZE: u64 = 128;
const DELMAP_ENTRY_SIZE: u64 = 32;
const DELS_PER_DELMAP: u64 = 8 * (DELMAP_ENTRY_SIZE - 6);
const HASH_LEN: usize = 26;
const SECTOR_SIZE: u64 = 1 << 20;
const FIRST_SLOT_OFFSET: u64 = 64;
const FIRST_SECTOR_OFFSET: u64 = 4 * 1024;
const SLOTS_IN_SECTOR: u64 = (SECTOR_SIZE - FIRST_SLOT_OFFSET) / SLOT_SIZE;
const EARLY_SECTOR_PERCENT: u64 = 80;
const MAX_SECTOR_PERCENT: u64 = 90;

const FREE_LIST_OFFSET: u64 = 8;
const NEXT_VALUE_LOGICAL_OFFSET: u64 = 16;
const FIRST_VALUE_LOGICAL_OFFSET: u64 = 24;
const NEXT_VALUE_PHYSICAL_OFFSET: u64 = 32;
const NEXT_DELMAP_PHYSICAL_OFFSET: u64 = 48;

const NO_VALUE: u64 = 0;

const WAL_MAGIC: u64 = 718984182412;

const IO_ERROR: &str = "IO error";

fn open_file(path: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(path)
        .expect(IO_ERROR)
}

pub struct HashTable {
    /// The node's salt for hashes
    salt: [u8; 32],
    /// The file that stores the database
    file: File,
    file_name: PathBuf,
    /// This structure represents the current transaction. All the reads and writes to the database
    /// are only possible in a context of a transaction.
    tx: TableTransaction,
    /// A map from the hashes of the keys to the sector offsets. All the sectors contain contiguous
    /// non-intersecting sets of hashes.
    ht_mapping: BTreeMap<[u8; 26], u64>,
    values_mapping: BTreeMap<u64, u64>,
    delmap_mapping: BTreeMap<u64, u64>,
    /// Number of new slots accross all sectors occupied since the last sector was resized. This is
    /// used to trigger an early resize if the number has been sufficiently large.
    writes_since_resize: u64,

    del_balance: i64,
}

pub struct FetchedPage {
    offset: u64,
    page: Vec<u8>,
    is_dirty: bool,
}

/// `TableTransaction` implements low level interaction with the database file. It allows
/// reading and writing some number of bytes at particular offsets, and provides consistency.
/// Specifically, if the process crashes, the writes that have happened before the call to
/// `commit` either will all be reflected in the state of the database, or all absent.
/// If the caller reads or writes some number of bytes at a particular offset, the caller must
/// always read and write exactly the same number of bytes at such offset.
pub struct TableTransaction {
    changes: BTreeMap<u64, Vec<u8>>,
    page: Option<FetchedPage>,
}

impl TableTransaction {
    fn new() -> Self {
        Self {
            changes: BTreeMap::new(),
            page: None,
        }
    }

    /// Removes all the changes the tx has tracked for the sector.
    fn reset_sector(&mut self, offset: u64) {
        let to_remove = self
            .changes
            .range(offset..offset + SECTOR_SIZE)
            .map(|x| *x.0)
            .collect::<Vec<_>>();
        for change in to_remove {
            self.changes.remove(&change);
        }
    }

    /// Stores the intent to write `data` at position `offset`.
    fn set(&mut self, offset: u64, data: Vec<u8>) {
        let len = data.len();
        if let Some(old_value) = self.changes.insert(offset, data) {
            assert_eq!(old_value.len(), len);
        }
    }

    /// Returns `len` bytes from the position `offset`. If the data at the offset has been
    /// overwritten as part of this transaction, returns the uncommitted value, otherwise fetches
    /// it from disk.
    fn get(&mut self, db_file: &mut File, offset: u64, len: u64) -> Vec<u8> {
        if let Some(data) = self.changes.get(&offset) {
            assert_eq!(data.len(), len as usize);
            return data.clone();
        }
        let within = (offset & (PAGE_SIZE - 1)) as usize;
        Self::fetch_page(&mut self.page, db_file, offset).page[within..within + len as usize]
            .to_vec()
    }

    pub fn get_num(&mut self, db_file: &mut File, offset: u64) -> u64 {
        let mut buf: [u8; 8] = [0; 8];
        buf.copy_from_slice(&self.get(db_file, offset, 8));
        u64::from_le_bytes(buf)
    }

    fn maybe_replay_log(&mut self, wal: &mut File) -> bool {
        let mut buf = [0u8; 8];
        if let Err(_) = wal.read_exact(&mut buf) {
            return false;
        }
        let num = u64::from_le_bytes(buf.clone());
        for _ in 0..num {
            if let Err(_) = wal.read_exact(&mut buf) {
                return false;
            }
            let offset = u64::from_le_bytes(buf.clone());
            if let Err(_) = wal.read_exact(&mut buf) {
                return false;
            }
            let len = u64::from_le_bytes(buf.clone());
            let mut data = vec![0u8; len as usize];
            if let Err(_) = wal.read_exact(&mut data) {
                return false;
            }
            self.set(offset, data);
        }
        if let Err(_) = wal.read_exact(&mut buf) {
            return false;
        }
        if u64::from_le_bytes(buf) != WAL_MAGIC {
            return false;
        }
        true
    }

    fn write_to_log(&mut self, wal: &mut File) {
        wal.write_all(&(self.changes.len() as u64).to_le_bytes())
            .expect(IO_ERROR);
        for (offset, data) in self.changes.iter() {
            wal.write_all(&offset.to_le_bytes()).expect(IO_ERROR);
            wal.write_all(&(data.len() as u64).to_le_bytes())
                .expect(IO_ERROR);
            wal.write_all(data).expect(IO_ERROR);
        }
        wal.write_all(&WAL_MAGIC.to_le_bytes()).expect(IO_ERROR);
    }

    /// Flushes all the changes to disk. Sorts the keys and inserts them in order, which, due to
    /// the logic of lazily fetching and flushing pages, ensures that each page is only written
    /// once.
    fn flush_changes(&mut self, db_path: PathBuf) {
        let mut changes = BTreeMap::new();
        std::mem::swap(&mut changes, &mut self.changes);

        let mut changes = changes.into_iter().collect::<Vec<_>>();

        let changes_grouped = (0..NUM_FLUSH_THREADS)
            .map(|i| changes.split_off(changes.len() - changes.len() / (NUM_FLUSH_THREADS - i)))
            .collect::<Vec<_>>();

        let threads = changes_grouped
            .into_iter()
            .map(|changes| {
                let db_path = db_path.clone();
                thread::spawn(move || {
                    let mut db_file = open_file(&db_path);
                    let mut page = None;
                    for (offset, data) in changes {
                        let within = (offset & (PAGE_SIZE - 1)) as usize;
                        let fetched_page = Self::fetch_page(&mut page, &mut db_file, offset);
                        fetched_page.page[within..within + data.len()].copy_from_slice(&data);
                        fetched_page.is_dirty = true;
                    }
                    Self::may_be_flush_page(&mut page, &mut db_file);
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().expect(IO_ERROR);
        }
        self.page = None;
    }

    /// Ensures that the `fetched_page` is the page that contains the offset, and returns the
    /// unwrapped `fetched_page`
    fn fetch_page<'a>(
        fetched_page: &'a mut Option<FetchedPage>,
        db_file: &mut File,
        mut offset: u64,
    ) -> &'a mut FetchedPage {
        offset &= !(PAGE_SIZE - 1);
        if fetched_page.as_ref().map_or(true, |x| x.offset != offset) {
            Self::may_be_flush_page(fetched_page, db_file);
            let mut page = vec![0u8; PAGE_SIZE as usize];
            db_file.seek(SeekFrom::Start(offset)).expect(IO_ERROR);
            db_file.read_exact(&mut page).expect(IO_ERROR);
            *fetched_page = Some(FetchedPage {
                offset,
                page,
                is_dirty: false,
            });
        }
        fetched_page.as_mut().unwrap()
    }

    fn may_be_flush_page(fetched_page: &mut Option<FetchedPage>, db_file: &mut File) {
        if let Some(page) = fetched_page {
            if page.is_dirty {
                db_file.seek(SeekFrom::Start(page.offset)).expect(IO_ERROR);
                db_file.write_all(&page.page).expect(IO_ERROR);
            }
            *fetched_page = None;
        }
    }
}

impl HashTable {
    pub fn new(db_path: PathBuf, salt: [u8; 32], wal: Option<&mut File>) -> Self {
        let mut file = open_file(&db_path);

        let mut ht_mapping = BTreeMap::new();
        //ht_mapping.insert([0; 26], FIRST_SECTOR_OFFSET);

        let mut values_mapping = BTreeMap::new();
        let mut delmap_mapping = BTreeMap::new();

        let file_len = file.metadata().expect(IO_ERROR).len();
        if file_len < FIRST_SECTOR_OFFSET + SECTOR_SIZE {
            // This is the first time we create this database
            const DESIRED_SIZE: u64 = FIRST_SECTOR_OFFSET + SECTOR_SIZE;
            let mut data = [0; DESIRED_SIZE as usize];
            data[0..8].copy_from_slice(&DESIRED_SIZE.to_le_bytes());
            data[NEXT_VALUE_PHYSICAL_OFFSET as usize..NEXT_VALUE_PHYSICAL_OFFSET as usize + 8]
                .copy_from_slice(&FIRST_SECTOR_OFFSET.to_le_bytes());
            data[NEXT_DELMAP_PHYSICAL_OFFSET as usize..NEXT_DELMAP_PHYSICAL_OFFSET as usize + 8]
                .copy_from_slice(&FIRST_SECTOR_OFFSET.to_le_bytes());
            data[FIRST_SECTOR_OFFSET as usize + 48..FIRST_SECTOR_OFFSET as usize + 56]
                .copy_from_slice(PAGE_TYPE_HT.to_le_bytes().as_ref());
            file.seek(SeekFrom::Start(0)).expect(IO_ERROR);
            file.write_all(&data).expect(IO_ERROR);
        }

        let mut tx = TableTransaction::new();

        if let Some(wal) = wal {
            if tx.maybe_replay_log(wal) {
                tx.flush_changes(db_path.clone());
            } else {
                tx = TableTransaction::new();
            }
        }
        let file_size = tx.get_num(&mut file, 0);

        let mut offset = FIRST_SECTOR_OFFSET;
        while offset < file_size {
            let page_type = tx.get_num(&mut file, offset + 48);
            if page_type == PAGE_TYPE_HT {
                ht_mapping.insert(tx.get(&mut file, offset, 26).try_into().unwrap(), offset);
            } else if page_type == PAGE_TYPE_VALUES {
                values_mapping.insert(tx.get_num(&mut file, offset), offset + VALUE_SIZE);
            } else if page_type == PAGE_TYPE_DELMAP {
                delmap_mapping.insert(tx.get_num(&mut file, offset), offset + FIRST_SLOT_OFFSET);
            } else {
                assert_eq!(page_type, PAGE_TYPE_FREE);
            }

            offset += SECTOR_SIZE;
        }
        assert_eq!(offset, file_size);

        HashTable {
            salt,
            file,
            file_name: db_path,
            tx,
            ht_mapping,
            values_mapping,
            delmap_mapping,
            writes_since_resize: 0,
            // `write_value` allocates new sectors whenever cur offset is on the sector boundary,
            // so setting to a sector boundary will force sector allocation on next write
            del_balance: 0,
        }
    }

    pub fn write_to_log(&mut self, wal: &mut File) {
        self.tx.write_to_log(wal);
    }

    pub fn flush_changes(&mut self) {
        self.tx.flush_changes(self.file_name.clone());
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let hash = self.get_hash(&key);
        let full_value_len = (hash.len() + value.len() + 8) as u64;
        let full_value_len_rounded_up = (full_value_len + VALUE_SIZE - 1) / VALUE_SIZE * VALUE_SIZE;
        let full_value = [
            hash.to_vec(),
            full_value_len.to_le_bytes().to_vec(),
            value,
            vec![0; (full_value_len_rounded_up - full_value_len) as usize],
        ]
        .concat();
        assert_eq!(full_value.len() as u64, full_value_len_rounded_up);

        let offset = self.write_value(full_value[0..128].try_into().unwrap());
        self.del_balance -= 2;
        for i in 1..full_value_len_rounded_up / VALUE_SIZE {
            let _ = self.write_value(
                full_value[(i * 128) as usize..(i * 128 + 128) as usize]
                    .try_into()
                    .unwrap(),
            );
            self.del_balance -= 2;
        }

        if let Some(old_offset) = self.ht_set_with_hash(hash, offset + 1) {
            self.delete_at_offset(old_offset - 1)
        }
    }

    pub fn print_stats(&mut self) {
        let logical_first_offset = self.tx.get_num(&mut self.file, FIRST_VALUE_LOGICAL_OFFSET);
        let logical_last_offset = self.tx.get_num(&mut self.file, NEXT_VALUE_LOGICAL_OFFSET);
        println!(
            "STATS: first: {} last: {}",
            logical_first_offset, logical_last_offset
        );
    }

    pub fn reset_del_balance(&mut self) {
        self.del_balance = 0;
    }

    pub fn get(&mut self, key: Vec<u8>) -> Option<Vec<u8>> {
        let hash = self.get_hash(&key);
        let (_, mut offset) = self.seek(hash);

        if offset == NO_VALUE {
            return None;
        }
        offset -= 1;

        let logical_first_offset = self.tx.get_num(&mut self.file, FIRST_VALUE_LOGICAL_OFFSET);
        if offset < logical_first_offset {
            assert!(false)
        }

        let mut values = vec![self.get_value(offset)];
        let len = u64::from_le_bytes(values[0][HASH_LEN..HASH_LEN + 8].try_into().unwrap());
        let mut remaining = len.saturating_sub(VALUE_SIZE);
        while remaining > 0 {
            offset += VALUE_SIZE;
            values.push(self.get_value(offset));
            remaining = remaining.saturating_sub(VALUE_SIZE);
        }

        Some(values.concat()[HASH_LEN + 8..len as usize].into())
    }

    fn delete_at_offset(&mut self, mut offset: u64) {
        let first_value = self.get_value(offset);
        let mut remaining =
            u64::from_le_bytes(first_value[HASH_LEN..HASH_LEN + 8].try_into().unwrap());

        while remaining > 0 {
            self.delete_value(offset);
            offset += VALUE_SIZE;
            remaining = remaining.saturating_sub(VALUE_SIZE);
            self.del_balance += 4;
        }

        while self.del_balance > 0 {
            let logical_first_offset = self.tx.get_num(&mut self.file, FIRST_VALUE_LOGICAL_OFFSET);
            let logical_next_offset = self.tx.get_num(&mut self.file, NEXT_VALUE_LOGICAL_OFFSET);
            let first_value = self.get_value(logical_first_offset);

            let mut remaining =
                u64::from_le_bytes(first_value[HASH_LEN..HASH_LEN + 8].try_into().unwrap());

            if logical_next_offset - logical_first_offset - remaining < VALUE_SIZE {
                // There's only one value, don't move it
                self.del_balance = 0;
                break;
            }

            if let Some((old_offset, new_offset)) = self.move_one_value() {
                let (ht_offset, mut stored_offset) =
                    self.seek(first_value[..HASH_LEN].try_into().unwrap());
                assert_ne!(stored_offset, NO_VALUE);
                stored_offset -= 1;
                assert_eq!(old_offset, stored_offset);
                self.tx.set(
                    ht_offset,
                    [
                        first_value[..HASH_LEN].as_ref(),
                        (1 + new_offset).to_le_bytes()[0..6].as_ref(),
                    ]
                    .concat(),
                );
            }
            remaining = remaining.saturating_sub(VALUE_SIZE);
            self.del_balance -= 1;

            while remaining > 0 {
                self.move_one_value();
                remaining = remaining.saturating_sub(VALUE_SIZE);
                self.del_balance -= 1;
            }
        }
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        let hash = self.get_hash(&key);
        let (_, mut offset) = self.seek(hash);

        if offset != NO_VALUE {
            offset -= 1;
            self.delete_at_offset(offset);
            self.ht_delete_with_hash(hash);
        }
    }

    /// Seeks the slot for a particular hash. Returns the offset of the slot, and the value
    pub fn seek(&mut self, hash: [u8; 26]) -> (u64, u64) {
        let mut slot = Self::get_slot(&hash);

        // unwrap here is safe, because the ht_mapping always contains 0x0
        let sector_offset = *self.ht_mapping.range(..=hash).next_back().unwrap().1;

        loop {
            let offset = sector_offset + slot * SLOT_SIZE + FIRST_SLOT_OFFSET;
            let data = self.tx.get(&mut self.file, offset, SLOT_SIZE);

            let value = Self::extract_value(&data);
            if value == NO_VALUE || data[..HASH_LEN] == hash[..] {
                return (offset, value);
            }

            slot += 1;
            if slot >= SLOTS_IN_SECTOR {
                slot = 0
            }
        }
    }

    pub fn ht_get(&mut self, key: Vec<u8>) -> Option<u64> {
        let hash = self.get_hash(&key);
        let (_offset, value) = self.seek(hash);
        if value != NO_VALUE {
            Some(value)
        } else {
            None
        }
    }

    pub fn ht_set(&mut self, key: Vec<u8>, new_value: u64) {
        let hash = self.get_hash(&key);
        self.ht_set_with_hash(hash, new_value);
    }

    fn ht_set_with_hash(&mut self, hash: [u8; 26], new_value: u64) -> Option<u64> {
        let (offset, old_value) = self.seek(hash);

        let data = [hash.as_ref(), &new_value.to_le_bytes()[..6]].concat();
        assert_eq!(data.len(), SLOT_SIZE as usize);
        self.tx.set(offset, data);

        if old_value == NO_VALUE {
            let sector_offset =
                ((offset - FIRST_SECTOR_OFFSET) & !(SECTOR_SIZE - 1)) + FIRST_SECTOR_OFFSET;

            let mut occ = self.tx.get_num(&mut self.file, sector_offset + 32);
            occ += 1;

            // If the segment is `MAX_SECTOR_PERCENT` occupied, resize it unconditionally.
            // Otherwise, resize it if it's `EARLY_SECTOR_PERCENT`, and `SLOTS_IN_SECTOR / 2` new
            // writes have happened across all sectors since the last resize. The latter is a
            // heuristic needed to space resizes in time (otherwise sectors grow with approximately
            // the same speed, and get resized close to each other in time).
            let resize = occ >= SLOTS_IN_SECTOR * MAX_SECTOR_PERCENT / 100
                || (occ >= SLOTS_IN_SECTOR * EARLY_SECTOR_PERCENT / 100
                    && self.writes_since_resize >= SLOTS_IN_SECTOR / 2);

            if !resize {
                self.writes_since_resize += 1;
                self.tx.set(sector_offset + 32, occ.to_le_bytes().to_vec());
            } else {
                self.writes_since_resize = 0;

                // We need to resize the sector. This process is done in three steps:
                // 1. Collect all the key-value pairs, and their hashes, and wipe out the content
                //    of the sector.
                let mut pairs: Vec<([u8; 26], u64)> = vec![];
                for slot in 0..SLOTS_IN_SECTOR {
                    let slot_offset = sector_offset + slot * SLOT_SIZE + FIRST_SLOT_OFFSET;
                    let data = self.tx.get(&mut self.file, slot_offset, SLOT_SIZE);
                    let value = Self::extract_value(&data);
                    if value != NO_VALUE {
                        pairs.push((
                            data[..HASH_LEN].try_into().unwrap(),
                            Self::extract_value(&data),
                        ))
                    }
                    self.tx.set(slot_offset, vec![0; SLOT_SIZE as usize]);
                }
                self.tx.set(sector_offset + 32, vec![0; 8]);

                // 2. Sort the hashes, and find the median hash. Create a new sector with such a key.
                pairs.sort_unstable();
                let median_hash = pairs[pairs.len() / 2].0;

                let sector_offset = self.allocate_sector(
                    vec![
                        median_hash.to_vec(),
                        vec![0u8; 8 + 8 + 6],
                        PAGE_TYPE_HT.to_le_bytes().to_vec(),
                        vec![0u8; 8],
                    ],
                    FIRST_SLOT_OFFSET,
                    SLOT_SIZE,
                );
                self.ht_mapping.insert(median_hash, sector_offset);

                // 3. Reinsert the data
                for (h, v) in pairs {
                    self.ht_set_with_hash(h, v);
                }
            }
            None
        } else {
            Some(old_value)
        }
    }

    pub fn ht_delete(&mut self, key: Vec<u8>) {
        let hash = self.get_hash(&key);
        self.ht_delete_with_hash(hash)
    }

    fn ht_delete_with_hash(&mut self, hash: [u8; 26]) {
        let (mut target_offset, old_value) = self.seek(hash);
        if old_value != NO_VALUE {
            let sector_offset =
                ((target_offset - FIRST_SECTOR_OFFSET) & !(SECTOR_SIZE - 1)) + FIRST_SECTOR_OFFSET;

            let occ = self.tx.get_num(&mut self.file, sector_offset + 32) - 1;
            self.tx.set(sector_offset + 32, occ.to_le_bytes().to_vec());

            let mut cur_offset = target_offset;
            loop {
                cur_offset += SLOT_SIZE;
                if ((cur_offset - FIRST_SECTOR_OFFSET) & (SECTOR_SIZE - 1)) == 0 {
                    cur_offset -= SECTOR_SIZE - FIRST_SLOT_OFFSET;
                }

                let data = self.tx.get(&mut self.file, cur_offset, SLOT_SIZE);
                if Self::extract_value(&data) == NO_VALUE {
                    self.tx.set(target_offset, vec![0; SLOT_SIZE as usize]);
                    break;
                }
                let desired_offset = sector_offset
                    + FIRST_SLOT_OFFSET
                    + SLOT_SIZE * Self::get_slot(&data[0..26].try_into().unwrap());

                let adjust = |x| {
                    if x < desired_offset {
                        x + SECTOR_SIZE - FIRST_SLOT_OFFSET
                    } else {
                        x
                    }
                };

                if adjust(cur_offset) > adjust(target_offset) {
                    self.tx.set(target_offset, data);
                    target_offset = cur_offset;
                }
            }
        }
    }

    fn is_value_at_offset_deleted(&mut self, logical_offset: u64) -> bool {
        let (sector_logical_offset, sector_physical_offset) = self
            .delmap_mapping
            .range(..=logical_offset)
            .next_back()
            .unwrap();
        let file_offset = sector_physical_offset
            + (logical_offset - sector_logical_offset) / VALUE_SIZE / DELS_PER_DELMAP
                * DELMAP_ENTRY_SIZE;

        let offset_within_delmap = (logical_offset / VALUE_SIZE) % DELS_PER_DELMAP;
        let cur_delmap = self.tx.get(&mut self.file, file_offset, DELMAP_ENTRY_SIZE);

        cur_delmap[offset_within_delmap as usize / 8] & (1 << (offset_within_delmap % 8)) == 0
    }

    fn move_one_value(&mut self) -> Option<(u64, u64)> {
        let logical_offset = self.tx.get_num(&mut self.file, FIRST_VALUE_LOGICAL_OFFSET);

        let new_logical_offset = logical_offset + VALUE_SIZE;
        self.tx.set(
            FIRST_VALUE_LOGICAL_OFFSET,
            new_logical_offset.to_le_bytes().to_vec(),
        );

        let ret = if !self.is_value_at_offset_deleted(logical_offset) {
            let value = self.get_value(logical_offset);
            let new_offset = self.write_value(value);
            Some((logical_offset, new_offset))
        } else {
            None
        };

        if new_logical_offset % (SECTOR_SIZE - VALUE_SIZE) == 0 {
            // The page that was holding the value being moved is now free
            let (&sector_logical_offset, &sector_physical_offset) = self
                .values_mapping
                .range(..=logical_offset)
                .next_back()
                .unwrap();

            assert_eq!(
                new_logical_offset,
                sector_logical_offset + SECTOR_SIZE - VALUE_SIZE
            );
            self.free_sector(sector_physical_offset - VALUE_SIZE);
            self.values_mapping.remove(&sector_logical_offset);
        }

        if new_logical_offset
            % ((SECTOR_SIZE - FIRST_SLOT_OFFSET) / DELMAP_ENTRY_SIZE * DELS_PER_DELMAP * VALUE_SIZE)
            == 0
        {
            // The page that was holding the delmap being moved is now free
            let (&sector_logical_offset, &sector_physical_offset) = self
                .delmap_mapping
                .range(..=logical_offset)
                .next_back()
                .unwrap();

            assert_eq!(
                new_logical_offset,
                sector_logical_offset
                    + (SECTOR_SIZE - FIRST_SLOT_OFFSET) / DELMAP_ENTRY_SIZE
                        * DELS_PER_DELMAP
                        * VALUE_SIZE
            );
            self.free_sector(sector_physical_offset - FIRST_SLOT_OFFSET);
        }

        ret
    }

    fn get_value(&mut self, logical_offset: u64) -> [u8; VALUE_SIZE as usize] {
        let (sector_logical_offset, sector_physical_offset) = self
            .values_mapping
            .range(..=logical_offset)
            .next_back()
            .unwrap();

        self.tx
            .get(
                &mut self.file,
                sector_physical_offset + logical_offset - sector_logical_offset,
                VALUE_SIZE,
            )
            .try_into()
            .unwrap()
    }

    fn write_value(&mut self, data: [u8; VALUE_SIZE as usize]) -> u64 {
        let cur_offset = self.tx.get_num(&mut self.file, NEXT_VALUE_LOGICAL_OFFSET);
        let mut next_value_physical_offset =
            self.tx.get_num(&mut self.file, NEXT_VALUE_PHYSICAL_OFFSET);
        let mut next_delmap_physical_offset =
            self.tx.get_num(&mut self.file, NEXT_DELMAP_PHYSICAL_OFFSET);

        self.tx.set(
            NEXT_VALUE_LOGICAL_OFFSET,
            (cur_offset + VALUE_SIZE).to_le_bytes().to_vec(),
        );

        if next_value_physical_offset % SECTOR_SIZE == FIRST_SECTOR_OFFSET {
            next_value_physical_offset = self.allocate_sector(
                vec![
                    cur_offset.to_le_bytes().to_vec(),
                    vec![0u8; 40],
                    PAGE_TYPE_VALUES.to_le_bytes().to_vec(),
                    vec![0u8; 8],
                    vec![0u8; 64],
                ],
                VALUE_SIZE,
                VALUE_SIZE,
            ) + VALUE_SIZE;
            self.values_mapping
                .insert(cur_offset, next_value_physical_offset);
        }

        self.tx.set(next_value_physical_offset, data.to_vec());
        next_value_physical_offset += VALUE_SIZE;
        self.tx.set(
            NEXT_VALUE_PHYSICAL_OFFSET,
            next_value_physical_offset.to_le_bytes().to_vec(),
        );

        let offset_within_delmap = (cur_offset / VALUE_SIZE) % DELS_PER_DELMAP;
        if offset_within_delmap == 0 {
            if next_delmap_physical_offset % SECTOR_SIZE == FIRST_SECTOR_OFFSET {
                next_delmap_physical_offset = self.allocate_sector(
                    vec![
                        cur_offset.to_le_bytes().to_vec(),
                        vec![0u8; 40],
                        PAGE_TYPE_DELMAP.to_le_bytes().to_vec(),
                        vec![0u8; 8],
                    ],
                    FIRST_SLOT_OFFSET,
                    DELMAP_ENTRY_SIZE,
                ) + FIRST_SLOT_OFFSET;
                self.delmap_mapping
                    .insert(cur_offset, next_delmap_physical_offset);
            }
            next_delmap_physical_offset += DELMAP_ENTRY_SIZE;
            self.tx.set(
                NEXT_DELMAP_PHYSICAL_OFFSET,
                next_delmap_physical_offset.to_le_bytes().to_vec(),
            );
        }
        let mut cur_delmap = self.tx.get(
            &mut self.file,
            next_delmap_physical_offset - DELMAP_ENTRY_SIZE,
            DELMAP_ENTRY_SIZE,
        );
        cur_delmap[offset_within_delmap as usize / 8] |= (1 << (offset_within_delmap % 8)) as u8;
        self.tx
            .set(next_delmap_physical_offset - DELMAP_ENTRY_SIZE, cur_delmap);

        cur_offset
    }

    fn delete_value(&mut self, logical_offset: u64) {
        let (sector_logical_offset, sector_physical_offset) = self
            .delmap_mapping
            .range(..=logical_offset)
            .next_back()
            .unwrap();
        let file_offset = sector_physical_offset
            + (logical_offset - sector_logical_offset) / VALUE_SIZE / DELS_PER_DELMAP
                * DELMAP_ENTRY_SIZE;

        let offset_within_delmap = (logical_offset / VALUE_SIZE) % DELS_PER_DELMAP;

        let mut cur_delmap = self.tx.get(&mut self.file, file_offset, DELMAP_ENTRY_SIZE);
        cur_delmap[offset_within_delmap as usize / 8] &= !((1 << (offset_within_delmap % 8)) as u8);
        self.tx.set(file_offset, cur_delmap);
    }

    // `prelude` should be split into vectors of the same size / alignment as will later be used by
    // the user of the page. It is expected that the prelude will have 8 bytes vectors at offsets
    // 48 and 56, the one at 48 containing the type of the page.
    fn allocate_sector(
        &mut self,
        prelude: Vec<Vec<u8>>,
        expected_prelude_size: u64,
        el_size: u64,
    ) -> u64 {
        let mut file_size = self.tx.get_num(&mut self.file, 0);

        let cur_free_offset = self.tx.get_num(&mut self.file, FREE_LIST_OFFSET);
        let ret = if cur_free_offset != 0 {
            let new_free_offset = self.tx.get_num(&mut self.file, cur_free_offset + 56);
            self.tx
                .set(FREE_LIST_OFFSET, new_free_offset.to_le_bytes().to_vec());
            cur_free_offset
        } else {
            self.file.seek(SeekFrom::Start(file_size)).expect(IO_ERROR);
            self.file
                .write_all(vec![0; SECTOR_SIZE as usize].as_ref())
                .expect(IO_ERROR);

            file_size += SECTOR_SIZE;
            self.tx.set(0, file_size.to_le_bytes().to_vec());

            file_size - SECTOR_SIZE
        };

        self.tx.reset_sector(ret);

        let mut offset = ret;
        for v in prelude {
            let v_len = v.len() as u64;
            self.tx.set(offset, v);
            offset += v_len;
        }

        assert_eq!(offset - ret, expected_prelude_size);

        while offset % SECTOR_SIZE != FIRST_SECTOR_OFFSET {
            self.tx.set(offset, vec![0u8; el_size as usize]);
            offset += el_size;
        }

        ret
    }

    fn free_sector(&mut self, offset: u64) {
        assert_eq!(offset & (SECTOR_SIZE - 1), FIRST_SECTOR_OFFSET);
        self.tx
            .set(offset + 48, PAGE_TYPE_FREE.to_le_bytes().to_vec());
        let cur_free_offset = self.tx.get_num(&mut self.file, FREE_LIST_OFFSET);
        self.tx
            .set(offset + 56, cur_free_offset.to_le_bytes().to_vec());
        self.tx.set(FREE_LIST_OFFSET, offset.to_le_bytes().to_vec());
    }

    fn extract_value(data: &Vec<u8>) -> u64 {
        let mut buf = [0u8; 8];
        buf[..6].copy_from_slice(&data[HASH_LEN..SLOT_SIZE as usize]);
        u64::from_le_bytes(buf)
    }

    fn get_hash(&self, key: &Vec<u8>) -> [u8; HASH_LEN] {
        let full_hash: [u8; 32] =
            blake3::hash([self.salt.as_ref(), key.as_ref()].concat().as_ref()).into();
        full_hash[..HASH_LEN].try_into().unwrap()
    }

    fn get_slot(hash: &[u8; 26]) -> u64 {
        let mut slice: [u8; 8] = [0; 8];
        slice.copy_from_slice(&hash[18..26]);
        u64::from_le_bytes(slice) % SLOTS_IN_SECTOR
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use rand::Rng;
    use tempdir::TempDir;

    #[test]
    fn test_sanity_db_free_list() {
        let tmp_dir = TempDir::new("example").unwrap();
        let mut db = HashTable::new(
            tmp_dir.path().join("db"),
            rand::thread_rng().gen::<[u8; 32]>(),
            None,
        );

        for i in 0..4 {
            assert_eq!(
                db.allocate_sector(vec![vec![0u8; VALUE_SIZE as usize]], VALUE_SIZE, VALUE_SIZE),
                (1 + i) * SECTOR_SIZE + FIRST_SECTOR_OFFSET
            );
        }

        for i in 0..4 {
            db.free_sector(2 * SECTOR_SIZE + FIRST_SECTOR_OFFSET);
            db.free_sector(4 * SECTOR_SIZE + FIRST_SECTOR_OFFSET);

            assert_eq!(
                db.allocate_sector(vec![vec![0u8; VALUE_SIZE as usize]], VALUE_SIZE, VALUE_SIZE),
                4 * SECTOR_SIZE + FIRST_SECTOR_OFFSET
            );

            assert_eq!(
                db.allocate_sector(vec![vec![0u8; VALUE_SIZE as usize]], VALUE_SIZE, VALUE_SIZE),
                2 * SECTOR_SIZE + FIRST_SECTOR_OFFSET
            );

            assert_eq!(
                db.allocate_sector(vec![vec![0u8; VALUE_SIZE as usize]], VALUE_SIZE, VALUE_SIZE),
                (5 + i) * SECTOR_SIZE + FIRST_SECTOR_OFFSET
            );
        }
    }

    #[test]
    fn test_sanity_db_values() {
        #[cfg(debug_assertions)]
        const ITERS: usize = 20000;
        #[cfg(not(debug_assertions))]
        const ITERS: usize = 500000;

        let tmp_dir = TempDir::new("example").unwrap();
        let mut db = HashTable::new(
            tmp_dir.path().join("db"),
            rand::thread_rng().gen::<[u8; 32]>(),
            None,
        );

        let mut byte: u8 = 17;
        let mut first_offset = db.write_value([byte; 128]);
        let mut next_offset = first_offset + 128;
        let mut next_del_offset = first_offset;
        let mut next_del_byte = byte;

        for iter in 0..(ITERS * 3) {
            byte = (byte + 1) % 250;

            assert_eq!(db.write_value([byte; 128]), next_offset);
            next_offset += 128;

            if iter >= ITERS {
                assert_eq!(db.get_value(next_del_offset), [next_del_byte; 128]);
                next_del_byte = (next_del_byte + 1) % 250;

                if (next_del_offset / 128) % 2 == 1 {
                    db.delete_value(next_del_offset);
                }
                next_del_offset += 128;
            }

            if iter >= ITERS * 2 {
                let maybe_offsets = db.move_one_value();
                if (first_offset / 128) % 2 == 0 {
                    assert_eq!(maybe_offsets, Some((first_offset, next_offset)));
                    next_offset += 128;
                } else {
                    assert_eq!(maybe_offsets, None);
                }

                first_offset += 128;
            }
        }
    }

    #[test]
    fn test_sanity_db_get_set() {
        let tmp_dir = TempDir::new("example").unwrap();
        let mut db = HashTable::new(
            tmp_dir.path().join("db"),
            rand::thread_rng().gen::<[u8; 32]>(),
            None,
        );

        db.set(vec![1, 2, 3, 4], vec![5, 6, 7, 8]);
        assert_eq!(db.get(vec![1, 2, 3, 4]), Some(vec![5, 6, 7, 8]));
        assert_eq!(db.get(vec![1, 2, 3, 5]), None);
    }
}
