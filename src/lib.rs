use std::path::{Path, PathBuf};
use std::io::prelude::{Read};
use std::error::Error;
use std::collections::{HashMap, HashSet};
use std::fs::File;

use walkdir::{WalkDir, DirEntry};

use std::hash::Hasher;
use twox_hash::XxHash64;

use indicatif::ProgressIterator;

const XXHASH_SEED_DEFAULT: u64 = 0;
const FILE_READ_BUFFER_SIZE: usize = 8192;
const SMALL_HASH_CHUNK_SIZE: usize = 1024;


type SizeDict = HashMap<FileSize, HashSet<FileIndex>>;
type SmallHashDict = HashMap<(FileSize, SmallHash), HashSet<FileIndex>>;
type FullHashDict = HashMap<FullHash, HashSet<FileIndex>>;

#[derive(Debug)]
pub struct JustOne {
    // hash_func: ,
    // ignore_error: bool = ignore_error
    // file_info: List[Tuple[FileIndex, Path, FileSize, Optional[HashValue], Optional[HashValue]]] = []
    file_info: Vec<FileInfo>,
    // file_index: Dict[Path, FileIndex] = {}
    file_index: HashMap<PathBuf, FileIndex>,
    // size_dict: DefaultDict[FileSize, Set[FileIndex]] = defaultdict(set)
    size_dict: SizeDict,
    // small_hash_dict: DefaultDict[Tuple[FileSize, HashValue], Set[FileIndex]] = defaultdict(set)
    small_hash_dict: SmallHashDict,
    // full_hash_dict: DefaultDict[HashValue, Set[FileIndex]] = defaultdict(set)
    full_hash_dict: FullHashDict,
}

#[derive(Debug)]
struct FileInfo {
    id: FileIndex,
    path: PathBuf,
    size: FileSize,
    small_hash: Option<SmallHash>,
    full_hash: Option<FullHash>,
}

type FileIndex = usize;
type FileSize = usize;
#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
struct SmallHash(u64);
#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
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

    pub fn update(&mut self, dir: impl AsRef<Path>) -> Result<&mut Self, Box<dyn Error>> {
        self.update_directory(dir, true)?;
        
        Ok(self)
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> {
        Ok(vec![])
    }

    fn update_directory(&mut self, dir: impl AsRef<Path>, ignore_error: bool) -> Result<HashSet<FileIndex>, Box<dyn Error>> {
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
    fn update_dir_entries<T>(&mut self, entries: T) -> Result<HashSet<FileIndex>, Box<dyn Error>> 
            where T: IntoIterator<Item=DirEntry> {       
        // size_dict_temp: DefaultDict[FileSize, Set[FileIndex]] = defaultdict(set)
        // small_hash_dict_temp: DefaultDict[Tuple[FileSize, HashValue], Set[FileIndex]] = defaultdict(set)
        // full_hash_dict_temp: DefaultDict[HashValue, Set[FileIndex]] = defaultdict(set)
        // duplicate_files_index: Set[FileIndex] = set()

        let mut size_dict_temp: SizeDict = HashMap::new();
        let mut small_hash_dict_temp: SmallHashDict = HashMap::new();
        let mut full_hash_dict_temp: FullHashDict = HashMap::new();
        let mut duplicate_files_index: HashSet<FileIndex> = HashSet::new();


        for entry in entries.into_iter().progress() {
            // let path: PathBuf = entry.path().into();
            let path: &Path = entry.path();
            let file_size = entry.metadata()?.len() as FileSize;
            let file_index = self.add_file_info(path, file_size, None, None)?;
            if let Some(set) = size_dict_temp.get_mut(&file_size) {
                set.insert(file_index);
            } else {
                let mut set = HashSet::new();
                set.insert(file_index);
                size_dict_temp.insert(file_size, set);
            }
            // println!("{}", entry.path().display());
        }

        for (file_size, file_index) in self.merge_size_dict(size_dict_temp)?.into_iter().progress() {
            let small_hash = self.get_small_hash(file_index)?;
            let key = (file_size, small_hash);
            if let Some(set) = small_hash_dict_temp.get_mut(&key) {
                set.insert(file_index);
            } else {
                let mut set = HashSet::new();
                set.insert(file_index);
                small_hash_dict_temp.insert(key, set);
            }
        }

        for file_index in self.merge_small_hash_dict(small_hash_dict_temp)?.into_iter().progress() {
            let full_hash = self.get_full_hash(file_index)?;
            if let Some(set) = full_hash_dict_temp.get_mut(&full_hash) {
                set.insert(file_index);
            } else {
                let mut set = HashSet::new();
                set.insert(file_index);
                full_hash_dict_temp.insert(full_hash, set);
            }
        }
        
        for file_index in self.merge_full_hash_dict(full_hash_dict_temp)?.into_iter().progress() {
            duplicate_files_index.insert(file_index);
        }
        
        Ok(duplicate_files_index)
    }

    fn add_file_info(&mut self, path: &Path, file_size: FileSize, small_hash: Option<SmallHash>, full_hash: Option<FullHash>) -> Result<FileIndex, Box<dyn Error>> {
        if let Some(&index) = self.file_index.get(path) {
            Ok(index)
        } else {
            let index = self.file_info.len();
            let old_index = self.file_index.insert(path.into(), index);
            debug_assert_eq!(old_index, None);
            self.file_info.push(FileInfo {
                id: index,
                path: path.into(),
                size: file_size as FileSize,
                small_hash,
                full_hash,
            });
            Ok(index)
        }
    }

    fn merge_size_dict(&mut self, size_dict_temp: SizeDict) -> Result<Vec<(FileSize, FileIndex)>, Box<dyn Error>> {
        // TODO: Use iterator like size_dict_temp.iter().map(|..| ...)...
        let mut merged: Vec<(FileSize, FileIndex)> = Vec::new();
        for (file_size, file_index_set_temp) in size_dict_temp {
            let file_index_set = self.size_dict.get_mut(&file_size).unwrap();
            let is_single = file_index_set.len() == 1;
            let union_set: HashSet<_> = file_index_set.union(&file_index_set_temp).cloned().collect(); // TODO: Try Extend::extend()
            if union_set.len() > 1 {
                let set = if is_single { union_set } else { file_index_set_temp };
                merged.extend(set.into_iter().map(|file_index| (file_size, file_index)));
            }
        }
        Ok(merged)
    }

    fn merge_small_hash_dict(&mut self, small_hash_dict_temp: SmallHashDict) -> Result<Vec<FileIndex>, Box<dyn Error>> {
        // TODO: Use iterator like small_hash_dict_temp.iter().map(|..| ...)...
        let mut merged: Vec<FileIndex> = Vec::new();
        for (file_size_and_small_hash, file_index_set_temp) in small_hash_dict_temp {
            let file_index_set = self.small_hash_dict.get_mut(&file_size_and_small_hash).unwrap();
            let is_single = file_index_set.len() == 1;
            let union_set: HashSet<_> = file_index_set.union(&file_index_set_temp).cloned().collect(); // TODO: Try Extend::extend()
            if union_set.len() > 1 {
                let set = if is_single { union_set } else { file_index_set_temp };
                merged.extend(set.into_iter());
            }
        }
        Ok(merged)
    }

    fn merge_full_hash_dict(&mut self, full_hash_dict_temp: FullHashDict) -> Result<Vec<FileIndex>, Box<dyn Error>> {
        // TODO: Use iterator like full_hash_dict_temp.iter().map(|..| ...)...
        let mut merged: Vec<FileIndex> = Vec::new();
        for (full_hash, file_index_set_temp) in full_hash_dict_temp {
            let file_index_set = self.full_hash_dict.get_mut(&full_hash).unwrap();
            let is_single = file_index_set.len() == 1;
            let union_set: HashSet<_> = file_index_set.union(&file_index_set_temp).cloned().collect(); // TODO: Try Extend::extend()
            if union_set.len() > 1 {
                let set = if is_single { union_set } else { file_index_set_temp };
                merged.extend(set.into_iter());
            }
        }
        Ok(merged)
    }

    fn get_small_hash(&mut self, file_index: FileIndex) -> Result<SmallHash, Box<dyn Error>> {
        let mut file_info = self.file_info.get_mut(file_index).unwrap();
        
        if let Some(hash) = file_info.small_hash {
            Ok(hash)
        } else {
            let mut f = File::open(&file_info.path)?;
            let hash = get_small_hash(&mut f)?;
            file_info.small_hash = Some(hash);
            Ok(hash)
        }
    }

    fn get_full_hash(&mut self, file_index: FileIndex) -> Result<FullHash, Box<dyn Error>> {
        let mut file_info = self.file_info.get_mut(file_index).unwrap();
        
        if let Some(hash) = file_info.full_hash {
            Ok(hash)
        } else {
            let mut f = File::open(&file_info.path)?;
            let hash = get_full_hash(&mut f)?;
            file_info.full_hash = Some(hash);
            Ok(hash)
        }
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