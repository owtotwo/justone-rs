use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, prelude::Read};
use std::path::{Path, PathBuf};

use walkdir::{DirEntry, WalkDir};

use std::hash::Hasher;
use twox_hash::XxHash64;

use indicatif::ProgressIterator;

use filecmp;

const XXHASH_SEED_DEFAULT: u64 = 0;
const FILE_READ_BUFFER_SIZE: usize = 8192;
const SMALL_HASH_CHUNK_SIZE: usize = 1024;

type SizeDict = HashMap<FileSize, HashSet<FileIndex>>;
type SmallHashDict = HashMap<(FileSize, SmallHash), HashSet<FileIndex>>;
type FullHashDict = HashMap<FullHash, HashSet<FileIndex>>;

pub struct JustOne {
    hasher_creator: Box<dyn Fn() -> Box<dyn Hasher>>,
    strict_level: StrictLevel,
    ignore_error: bool,
    ignore_files: Vec<PathBuf>,
    file_info: Vec<FileInfo>,
    file_index: HashMap<PathBuf, FileIndex>,
    size_dict: SizeDict,
    small_hash_dict: SmallHashDict,
    full_hash_dict: FullHashDict,
}

#[derive(Debug)]
pub enum StrictLevel {
    Common,
    Shallow,
    ByteByByte,
}

#[derive(Debug)]
pub enum JustOneError {
    IOError {
        files: Vec<PathBuf>,
        error: io::Error,
    },
    WalkdirError(walkdir::Error),
    GeneralError(Box<dyn Error>),
    UnknownError,
}

macro_rules! io_error {
    ($err:expr $(, $file:expr) *) => {{
        #[cfg(debug_assertions)]
        eprintln!("[DEBUG:io_error!] {}:{}:{}", file!(), line!(), column!());
        #[cfg(debug_assertions)]
        eprintln!("[DEBUG:io_error!] {}:{}:{}", file!(), line!(), column!()); // redundant
        JustOneError::IOError {
            files: vec![$(($file.as_ref() as &Path).to_path_buf(),)*],
            error: $err,
        }
    }};
}

macro_rules! walkdir_error {
    ($err:expr) => {{
        #[cfg(debug_assertions)]
        eprintln!(
            "[DEBUG:walkdir_error!] {}:{}:{}",
            file!(),
            line!(),
            column!()
        );
        #[cfg(debug_assertions)]
        eprintln!(
            "[DEBUG:walkdir_error!] {}:{}:{}",
            file!(),
            line!(),
            column!()
        ); // redundant
        JustOneError::WalkdirError($err)
    }};
}

impl fmt::Display for JustOneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JustOneError::IOError { files, error } => {
                match files.len() {
                    0 => (),
                    1 => format!("`{}` ", files[0].display()).fmt(f)?,
                    _ => format!(
                        "[{}]\n",
                        files
                            .iter()
                            .map(|p| p.display().to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                    .fmt(f)?,
                };
                error.fmt(f)
            }
            JustOneError::WalkdirError(e) => e.fmt(f),
            JustOneError::GeneralError(e) => e.fmt(f),
            JustOneError::UnknownError => write!(f, "Unknown Error occurred"),
        }
    }
}

impl Error for JustOneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            JustOneError::IOError { files: _, error } => Some(error),
            JustOneError::WalkdirError(e) => Some(e),
            JustOneError::GeneralError(e) => Some(e.as_ref()),
            JustOneError::UnknownError => None,
        }
    }
}

impl From<io::Error> for JustOneError {
    fn from(err: io::Error) -> Self {
        JustOneError::IOError {
            files: Vec::new(),
            error: err,
        }
    }
}

impl From<Box<dyn Error>> for JustOneError {
    fn from(err: Box<dyn Error>) -> Self {
        JustOneError::GeneralError(err)
    }
}

impl From<walkdir::Error> for JustOneError {
    fn from(err: walkdir::Error) -> Self {
        JustOneError::WalkdirError(err)
    }
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

impl Default for JustOne {
    fn default() -> Self {
        JustOne {
            hasher_creator: Box::new(|| Box::new(XxHash64::with_seed(XXHASH_SEED_DEFAULT))),
            strict_level: StrictLevel::default(),
            ignore_error: false,
            ignore_files: Vec::new(),
            file_info: Vec::new(),
            file_index: HashMap::new(),
            size_dict: HashMap::new(),
            small_hash_dict: HashMap::new(),
            full_hash_dict: HashMap::new(),
        }
    }
}

impl Default for StrictLevel {
    fn default() -> Self {
        StrictLevel::Common
    }
}

/// Return a default hasher creator (XxHash64 with constant int seed)
pub fn default_hasher_creator() -> Box<dyn Fn() -> Box<dyn Hasher>> {
    Box::new(|| Box::new(XxHash64::with_seed(XXHASH_SEED_DEFAULT)))
}

impl JustOne {
    pub fn new() -> Self {
        JustOne::default()
    }

    pub fn with_config(strict_level: StrictLevel, ignore_error: bool) -> Self {
        JustOne {
            hasher_creator: default_hasher_creator(),
            strict_level,
            ignore_error,
            ..JustOne::default()
        }
    }

    pub fn with_full_config(
        hasher_creator: Box<dyn Fn() -> Box<dyn Hasher>>,
        strict_level: StrictLevel,
        ignore_error: bool,
    ) -> Self {
        JustOne {
            hasher_creator,
            strict_level,
            ignore_error,
            ..JustOne::default()
        }
    }

    pub fn update(&mut self, dir: impl AsRef<Path>) -> Result<&mut Self, JustOneError> {
        self.update_directory(dir)?;

        Ok(self)
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<&Path>>, JustOneError> {
        match self.strict_level {
            StrictLevel::Common => self.duplicates_common(),
            StrictLevel::Shallow => self.duplicates_strict(true),
            StrictLevel::ByteByByte => self.duplicates_strict(false),
        }
    }

    fn duplicates_common(&self) -> Result<Vec<Vec<&Path>>, JustOneError> {
        let mut dups: Vec<Vec<&Path>> = Vec::with_capacity(self.full_hash_dict.len());
        for (_, file_index_set) in &self.full_hash_dict {
            let set_size = file_index_set.len();
            debug_assert!(set_size >= 1);
            if set_size == 1 {
                continue;
            }
            let mut dup = Vec::with_capacity(set_size);
            for file_index in file_index_set {
                dup.push(self.get_file_path_by_index(file_index.clone()));
            }
            dups.push(dup);
        }
        Ok(dups)
    }

    fn duplicates_strict(&self, shallow: bool) -> Result<Vec<Vec<&Path>>, JustOneError> {
        let dups = self.duplicates_common()?;
        let mut diff_files: Vec<Vec<&Path>> = Vec::new();
        for dup in dups {
            for file in dup {
                for same_files in &mut diff_files {
                    let first_file = same_files[0];
                    if file_cmp(file, first_file, shallow)? {
                        same_files.push(file);
                        break;
                    }
                }
                diff_files.push(vec![file]);
            }
        }

        Ok(diff_files)
    }

    fn update_directory(
        &mut self,
        dir: impl AsRef<Path>,
    ) -> Result<HashSet<FileIndex>, JustOneError> {
        let mut entries = Vec::new();
        for entry in WalkDir::new(dir) {
            let entry = match entry {
                Ok(val) => val,
                Err(e) => {
                    if self.ignore_error {
                        if let Some(path) = e.path() {
                            self.ignore_files.push(path.to_path_buf());
                        }
                        continue;
                    } else {
                        return Err(walkdir_error!(e));
                    }
                }
            };
            // TODO: check if is symlink
            if entry.file_type().is_file() {
                entries.push(entry);
            }
        }
        self.update_dir_entries(entries)
    }
    fn update_dir_entries<T>(&mut self, entries: T) -> Result<HashSet<FileIndex>, JustOneError>
    where
        T: IntoIterator<Item = DirEntry>,
    {
        let mut size_dict_temp: SizeDict = HashMap::new();
        let mut small_hash_dict_temp: SmallHashDict = HashMap::new();
        let mut full_hash_dict_temp: FullHashDict = HashMap::new();
        let mut duplicate_files_index: HashSet<FileIndex> = HashSet::new();

        for entry in entries.into_iter().progress() {
            let path: &Path = entry.path();
            let file_size = entry.metadata().map_err(|e| walkdir_error!(e))?.len() as FileSize;
            let file_index = self.add_file_info(path, file_size, None, None);
            size_dict_temp
                .entry(file_size)
                .or_insert_with(|| HashSet::new())
                .insert(file_index);
        }

        for (file_size, file_index) in self.merge_size_dict(size_dict_temp).into_iter().progress() {
            let small_hash = match self.get_small_hash(file_index) {
                Ok(val) => val,
                Err(e) => {
                    if self.ignore_error {
                        self.ignore_files
                            .push(self.file_info.get(file_index).unwrap().path.clone());
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
            let key = (file_size, small_hash);
            small_hash_dict_temp
                .entry(key)
                .or_insert_with(|| HashSet::new())
                .insert(file_index);
        }

        for file_index in self
            .merge_small_hash_dict(small_hash_dict_temp)
            .into_iter()
            .progress()
        {
            let full_hash = match self.get_full_hash(file_index) {
                Ok(val) => val,
                Err(e) => {
                    if self.ignore_error {
                        self.ignore_files
                            .push(self.file_info.get(file_index).unwrap().path.clone());
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
            full_hash_dict_temp
                .entry(full_hash)
                .or_insert_with(|| HashSet::new())
                .insert(file_index);
        }

        for file_index in self
            .merge_full_hash_dict(full_hash_dict_temp)
            .into_iter()
            .progress()
        {
            duplicate_files_index.insert(file_index);
        }

        Ok(duplicate_files_index)
    }

    fn add_file_info(
        &mut self,
        path: &Path,
        file_size: FileSize,
        small_hash: Option<SmallHash>,
        full_hash: Option<FullHash>,
    ) -> FileIndex {
        if let Some(&index) = self.file_index.get(path) {
            index
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
            index
        }
    }

    fn get_file_path_by_index(&self, file_index: FileIndex) -> &Path {
        &self.file_info.get(file_index).unwrap().path
    }

    fn merge_size_dict(&mut self, size_dict_temp: SizeDict) -> Vec<(FileSize, FileIndex)> {
        let mut merged: Vec<(FileSize, FileIndex)> = Vec::new();
        for (file_size, file_index_set_temp) in size_dict_temp {
            if !self.size_dict.contains_key(&file_size) {
                self.size_dict.insert(file_size, HashSet::new());
            }
            let file_index_set = self.size_dict.get_mut(&file_size).unwrap();
            let is_single = file_index_set.len() == 1;
            file_index_set.extend(file_index_set_temp.iter());
            if file_index_set.len() > 1 {
                let set = if is_single {
                    &*file_index_set
                } else {
                    &file_index_set_temp
                };
                merged.extend(set.iter().map(|&file_index| (file_size, file_index)));
            }
        }
        merged
    }

    fn merge_small_hash_dict(&mut self, small_hash_dict_temp: SmallHashDict) -> Vec<FileIndex> {
        let mut merged: Vec<FileIndex> = Vec::new();
        for (file_size_and_small_hash, file_index_set_temp) in small_hash_dict_temp {
            if !self.small_hash_dict.contains_key(&file_size_and_small_hash) {
                self.small_hash_dict
                    .insert(file_size_and_small_hash, HashSet::new());
            }
            let file_index_set = self
                .small_hash_dict
                .get_mut(&file_size_and_small_hash)
                .unwrap();
            let is_single = file_index_set.len() == 1;
            file_index_set.extend(file_index_set_temp.iter());
            if file_index_set.len() > 1 {
                let set = if is_single {
                    &*file_index_set
                } else {
                    &file_index_set_temp
                };
                merged.extend(set.iter());
            }
        }
        merged
    }

    fn merge_full_hash_dict(&mut self, full_hash_dict_temp: FullHashDict) -> Vec<FileIndex> {
        let mut merged: Vec<FileIndex> = Vec::new();
        for (full_hash, file_index_set_temp) in full_hash_dict_temp {
            if !self.full_hash_dict.contains_key(&full_hash) {
                self.full_hash_dict.insert(full_hash, HashSet::new());
            }
            let file_index_set = self.full_hash_dict.get_mut(&full_hash).unwrap();
            let is_single = file_index_set.len() == 1;
            file_index_set.extend(file_index_set_temp.iter());
            if file_index_set.len() > 1 {
                let set = if is_single {
                    &*file_index_set
                } else {
                    &file_index_set_temp
                };
                merged.extend(set.iter());
            }
        }
        merged
    }

    fn get_small_hash(&mut self, file_index: FileIndex) -> Result<SmallHash, JustOneError> {
        let mut file_info = self.file_info.get_mut(file_index).unwrap();

        if let Some(hash) = file_info.small_hash {
            Ok(hash)
        } else {
            let path = &file_info.path;
            let mut f = File::open(path).map_err(|e| io_error!(e, path))?;
            let hasher_creator = self.hasher_creator.as_ref();
            let hasher = hasher_creator();
            let hash = get_small_hash(&mut f, hasher).map_err(|e| io_error!(e, path))?;
            file_info.small_hash = Some(hash);
            Ok(hash)
        }
    }

    fn get_full_hash(&mut self, file_index: FileIndex) -> Result<FullHash, JustOneError> {
        let mut file_info = self.file_info.get_mut(file_index).unwrap();

        if let Some(hash) = file_info.full_hash {
            Ok(hash)
        } else {
            let path = &file_info.path;
            let mut f = File::open(path).map_err(|e| io_error!(e, path))?;
            let hasher_creator = self.hasher_creator.as_ref();
            let hasher = hasher_creator();
            let hash = get_full_hash(&mut f, hasher).map_err(|e| io_error!(e, path))?;
            file_info.full_hash = Some(hash);
            Ok(hash)
        }
    }
}

fn get_small_hash(f: &mut dyn Read, mut hasher: Box<dyn Hasher>) -> Result<SmallHash, io::Error> {
    let mut buffer = [0; SMALL_HASH_CHUNK_SIZE];
    let read_size = f.read(&mut buffer)?;
    hasher.write(&buffer[..read_size]);
    Ok(SmallHash(hasher.finish()))
}

fn get_full_hash(f: &mut dyn Read, mut hasher: Box<dyn Hasher>) -> Result<FullHash, io::Error> {
    let mut buffer = [0; FILE_READ_BUFFER_SIZE];
    loop {
        let read_size = f.read(&mut buffer)?;
        if read_size == 0 {
            break;
        }
        hasher.write(&buffer[..read_size]);
    }
    Ok(FullHash(hasher.finish()))
}

fn file_cmp(
    file_a: impl AsRef<Path>,
    file_b: impl AsRef<Path>,
    shallow: bool,
) -> Result<bool, JustOneError> {
    Ok(filecmp::cmp(&file_a, &file_b, shallow).map_err(|e| io_error!(e, file_a, file_b))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_small_hash() {
        let hasher_creator = default_hasher_creator();

        let mut f = &[b'0'; 12345][..];
        let SmallHash(hash_val) = get_small_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("908a9517d970b2c6", format!("{:016x}", hash_val)); // xxh64

        let mut f = &b"abc"[..];
        let SmallHash(hash_val) = get_small_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val)); // xxh64

        let mut f = &b""[..];
        let SmallHash(hash_val) = get_small_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("ef46db3751d8e999", format!("{:016x}", hash_val)); // xxh64
    }

    #[test]
    fn test_get_full_hash() {
        let hasher_creator = default_hasher_creator();

        let mut f = &[b'0'; 12345][..];
        let FullHash(hash_val) = get_full_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("8052320d3bcad6a7", format!("{:016x}", hash_val)); // xxh64

        let mut f = &b"abc"[..];
        let FullHash(hash_val) = get_full_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("44bc2cf5ad770999", format!("{:016x}", hash_val)); // xxh64

        let mut f = &b""[..];
        let FullHash(hash_val) = get_full_hash(&mut f, hasher_creator()).unwrap();
        assert_eq!("ef46db3751d8e999", format!("{:016x}", hash_val)); // xxh64
    }
}
