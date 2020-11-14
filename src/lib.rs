use std::path::{Path, PathBuf};
use std::io::prelude::{Read};
use std::error::Error;
use std::collections::{HashMap, HashSet};

use walkdir::{WalkDir, DirEntry};

use std::hash::Hasher;
use twox_hash::XxHash64;

const XXHASH_SEED_DEFAULT: u64 = 0;
const FILE_READ_BUFFER_SIZE: usize = 8192;
const SMALL_HASH_CHUNK_SIZE: usize = 1024;

#[derive(Debug)]
pub struct JustOne<'a> {
    // hash_func: ,
    // ignore_error: bool = ignore_error
    // file_info: List[Tuple[FileIndex, Path, FileSize, Optional[HashValue], Optional[HashValue]]] = []
    file_info: Vec<FileInfo<'a>>,
    // file_index: Dict[Path, FileIndex] = {}
    file_index: HashMap<PathBuf, FileIndex>,
    // size_dict: DefaultDict[FileSize, Set[FileIndex]] = defaultdict(set)
    size_dict: HashMap<FileSize, HashSet<FileIndex>>,
    // small_hash_dict: DefaultDict[Tuple[FileSize, HashValue], Set[FileIndex]] = defaultdict(set)
    small_hash_dict: HashMap<(FileSize, SmallHash), HashSet<FileIndex>>,
    // full_hash_dict: DefaultDict[HashValue, Set[FileIndex]] = defaultdict(set)
    full_hash_dict: HashMap<FullHash, HashSet<FileIndex>>,
}

#[derive(Debug)]
struct FileInfo<'a> {
    id: FileIndex,
    path: &'a Path,
    size: FileSize,
    small_hash_val: SmallHash,
    full_hash_val: FullHash,
}

type FileIndex = usize;
type FileSize = usize;
#[derive(Debug)]
struct SmallHash(u64);
#[derive(Debug)]
struct FullHash(u64);

impl<'a> JustOne<'a> {
    pub fn new() -> Self {
        JustOne {
            file_info: Vec::new(),
            file_index: HashMap::new(),
            size_dict: HashMap::new(),
            small_hash_dict: HashMap::new(),
            full_hash_dict: HashMap::new(),
        }
    }

    pub fn update(&mut self, dir: impl AsRef<Path>) -> Result<(), Box<dyn Error>> {
        self.update_directory(dir, true).unwrap();
        
        Ok(())
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> {
        Ok(vec![])
    }

    fn update_directory(&mut self, dir: impl AsRef<Path>, ignore_error: bool) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> {
        if !ignore_error {
            let mut entries = Vec::new();
            for entry in WalkDir::new(dir) {
                let entry = entry?;
                // println!("{}", entry.path().display());
                entries.push(entry);
            }
            self.update_dir_entries(entries)
        } else {
            self.update_dir_entries(
                // Iterate over all entries and ignore any errors that may arise
                // (e.g., This code below will silently skip directories that the owner of the running process does not have permission to access.)
                WalkDir::new(dir)
                    .into_iter()
                    .filter_map(|e| e.ok())
                    .filter(|e| !e.file_type().is_dir())
            )
        }
    }
    fn update_dir_entries<T>(&self, entries: T) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> 
            where T: IntoIterator<Item=DirEntry> {       
        for entry in entries.into_iter() {
            println!("{}", entry.path().display());
        }
        Ok(vec![])
    }
}

fn get_small_hash(f: &mut dyn Read) -> Result<SmallHash, Box<dyn Error>> {
    let mut buffer = [0; SMALL_HASH_CHUNK_SIZE];
    let mut hasher = XxHash64::with_seed(XXHASH_SEED_DEFAULT); // TODO: Use xxh3_128
    let read_size = f.read(&mut buffer)?;
    hasher.write(&buffer[..read_size]);
    Ok(SmallHash(hasher.finish()))
}

fn get_full_hash(f: &mut dyn Read) -> Result<FullHash, Box<dyn Error>> {
    let mut hasher = XxHash64::with_seed(XXHASH_SEED_DEFAULT); // TODO: Use xxh3_128
    let mut buffer = [0; FILE_READ_BUFFER_SIZE];
    loop {
        let read_size = f.read(&mut buffer)?;
        if read_size == 0 { break }
        hasher.write(&buffer[..read_size]);
    }
    Ok(FullHash(hasher.finish()))
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_get_small_hash() {
        let mut f = &[b'0'; 12345][..];
        let SmallHash(hash_val) = get_small_hash(&mut f).unwrap();        
        assert_eq!("908a9517d970b2c6", format!("{:016x}", hash_val)); // xxh64

        let mut f = File::open("test_data/Against the Current - Legends Never Die-2017英雄联盟全球总决赛主题曲.mp3").unwrap();
        let SmallHash(hash_val) = get_small_hash(&mut f).unwrap();        
        assert_eq!("fb95eaebae131262", format!("{:016x}", hash_val)); // xxh64

        let mut f = File::open("test_data/test.txt").unwrap();
        let SmallHash(hash_val) = get_small_hash(&mut f).unwrap();        
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val)); // xxh64

        let mut f = File::open("test_data/empty.txt").unwrap();
        let SmallHash(hash_val) = get_small_hash(&mut f).unwrap();
        assert_eq!("ef46db3751d8e999", format!("{:016x}", hash_val)); // xxh64
    }

    #[test]
    fn test_get_full_hash() {
        let mut f = &[b'0'; 12345][..];
        let FullHash(hash_val) = get_full_hash(&mut f).unwrap();        
        assert_eq!("8052320d3bcad6a7", format!("{:016x}", hash_val)); // xxh64
        
        let mut f = File::open("test_data/Against the Current - Legends Never Die-2017英雄联盟全球总决赛主题曲.mp3").unwrap();
        let FullHash(hash_val) = get_full_hash(&mut f).unwrap();
        assert_eq!("2b18bac92063d35f", format!("{:016x}", hash_val)); // xxh64

        let mut f = File::open("test_data/test.txt").unwrap();
        let FullHash(hash_val) = get_full_hash(&mut f).unwrap();
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val)); // xxh64

        let mut f = File::open("test_data/empty.txt").unwrap();
        let FullHash(hash_val) = get_full_hash(&mut f).unwrap();
        assert_eq!("ef46db3751d8e999", format!("{:016x}", hash_val)); // xxh64
    }
}