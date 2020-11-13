use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::prelude::{Read};
use std::error::Error;
use std::collections::{HashMap, HashSet};

use walkdir::WalkDir;

use std::hash::{Hasher};
use twox_hash::{Xxh3Hash64, XxHash64};

const XXHASH_SEED_DEFAULT: u64 = 0;
const FILE_READ_BUFFER_SIZE: usize = 8192;
const SMALL_HASH_CHUNK_SIZE: usize = 1024;

#[derive(Debug)]
pub struct JustOne {
    // hash_func: ,
    // ignore_error: bool = ignore_error
    // file_info: List[Tuple[FileIndex, Path, FileSize, Optional[HashValue], Optional[HashValue]]] = []
    file_info: Vec<FileInfo>,
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
struct FileInfo {
    id: FileIndex,
    path: Box<Path>,
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

impl JustOne {
    pub fn new() -> Self {
        JustOne {
            file_info: Vec::new(),
            file_index: HashMap::new(),
            size_dict: HashMap::new(),
            small_hash_dict: HashMap::new(),
            full_hash_dict: HashMap::new(),
        }
    }

    pub fn update(&mut self, dir: &Path) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> {
        Ok(vec![])
    }
}

fn get_small_hash(mut f: File) -> Result<SmallHash, Box<dyn Error>> {
    let mut chunk_buffer = [0; SMALL_HASH_CHUNK_SIZE];
    let mut hasher = XxHash64::with_seed(XXHASH_SEED_DEFAULT); // TODO: Use xxh3_128
    let read_size = f.read(&mut chunk_buffer)?;
    hasher.write(&chunk_buffer[..read_size]);
    // hasher.write(b"test");
    // let hex_buffer: String = chunk_buffer.iter()
    //     .as_slice()
    //     .chunks(16)
    //     .map(|chunk| chunk.iter().map(|byte| format!("{:02x}", byte)).collect::<String>())
    //     .map(|line| format!("{}\n", line)).collect();
    // println!("buffer is {}", hex_buffer);

    Ok(SmallHash(hasher.finish()))
}

fn get_full_hash(mut f: File) -> Result<FullHash, Box<dyn Error>> {
    let mut chunk_buffer = [0; SMALL_HASH_CHUNK_SIZE];
    let mut hasher = XxHash64::with_seed(XXHASH_SEED_DEFAULT); // TODO: Use xxh3_128
    loop {
        let read_size = f.by_ref().take(SMALL_HASH_CHUNK_SIZE as u64).read(&mut chunk_buffer)?;
        hasher.write(&chunk_buffer[..read_size]);
        if read_size == 0 { break; };
    }

    Ok(FullHash(hasher.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use twox_hash::{XxHash32, XxHash64, Xxh3Hash64, Xxh3Hash128};
    use twox_hash::xxh3::*;

    #[test]
    fn test_get_small_hash() {
        let f = File::open("test_data/Against the Current - Legends Never Die-2017英雄联盟全球总决赛主题曲.mp3").unwrap();
        let SmallHash(hash_val) = get_small_hash(f).unwrap();        
        assert_eq!("fb95eaebae131262", format!("{:016x}", hash_val));

        let f = File::open("test_data/test.txt").unwrap();
        let SmallHash(hash_val) = get_small_hash(f).unwrap();        
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val));

        // let mut hasher = Xxh3Hash64::default();
        // hasher.write(b"xxhash");
        // let val = hasher.finish();
        // println!("Xxh3Hash64: {:x}", val);

        // let mut hasher = Xxh3Hash128::default();
        // hasher.write(b"xxhash");
        // let val = hasher.finish();
        // println!("Xxh3Hash128: {:x}", val);

        // let mut hasher = XxHash32::default();
        // hasher.write(b"xxhash");
        // let val = hasher.finish();
        // println!("XxHash32: {:x}", val);

        // let mut hasher = XxHash64::default();
        // hasher.write(b"xxhash");
        // let val = hasher.finish();
        // println!("XxHash64: {:x}", val);

        // let val = hash64_with_seed(b"xxhash", 0);
        // println!("hash64_with_seed: {:x}", val);

        // let val = hash128_with_seed(b"xxhash", 0);
        // println!("hash128_with_seed: {:x}", val);

        // xxh3_64 small hex: 8e881cdca1df7770
        // xxh3_64 small int: 10270490684152575856
        // xxh3_128 small hex: c97dd49cdbf0726a8e881cdca1df7770
        // xxh3_128 small int: 267828176558582169949584563257206601584
    }

    #[test]
    fn test_get_full_hash() {
        let f = File::open("test_data/Against the Current - Legends Never Die-2017英雄联盟全球总决赛主题曲.mp3").unwrap();
        let FullHash(hash_val) = get_full_hash(f).unwrap();
        assert_eq!("2b18bac92063d35f", format!("{:016x}", hash_val));

        let f = File::open("test_data/test.txt").unwrap();
        let FullHash(hash_val) = get_full_hash(f).unwrap();
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val));
    }
}