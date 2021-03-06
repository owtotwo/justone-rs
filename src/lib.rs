use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::result;

use walkdir::{DirEntry, WalkDir};

use std::hash::Hasher;
use twox_hash::XxHash64;

use indicatif::ProgressIterator;

use filecmp;

const FOLLOW_LINKS_DEFAULT: bool = false;
const IGNORE_ERROR_DEFAULT: bool = false;
const IGNORE_SYMLINK_DEFAULT: bool = false;
const XXHASH_SEED_DEFAULT: u64 = 0;
const FILE_READ_BUFFER_SIZE: usize = 8192;
const SMALL_HASH_CHUNK_SIZE: usize = 1024;

type SizeDict = HashMap<FileSize, HashSet<FileIndex>>;
type SmallHashDict = HashMap<(FileSize, SmallHash), HashSet<FileIndex>>;
type FullHashDict = HashMap<FullHash, HashSet<FileIndex>>;
type SymlinkHashDict = HashMap<SymlinkContent, HashSet<SymlinkPath>>;

pub type Result<T> = result::Result<T, JustOneError>;

pub struct JustOne {
    hasher_creator: Box<dyn Fn() -> Box<dyn Hasher>>,
    strict_level: StrictLevel,
    /// If true, PermissionDenied or other IO Error will be ignored
    ignore_error: bool,
    /// Files which were ignored if `ignore_error` is true
    ignored_files: Vec<PathBuf>,
    /// If true, symlink-type file will be ignored, and `follow_links` will be set false
    ignore_symlink: bool,
    /// If true, it will traverse symbolic link to dest file when deal with symlink
    follow_links: bool,
    file_info: Vec<FileInfo>,
    file_index: HashMap<PathBuf, FileIndex>,
    size_dict: SizeDict,
    small_hash_dict: SmallHashDict,
    full_hash_dict: FullHashDict,
    symlink_hash_dict: SymlinkHashDict,
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
}

macro_rules! io_error {
    ($err:expr $(, $file:expr) *) => {{
        #[cfg(debug_assertions)]
        // '\n' in tail for the printed-line covering by '\r'
        eprintln!("[DEBUG:io_error!] {}:{}:{}\n", file!(), line!(), column!());
        JustOneError::IOError {
            files: vec![$(($file.as_ref() as &Path).to_path_buf(),)*],
            error: $err,
        }
    }};
}

macro_rules! walkdir_error {
    ($err:expr) => {{
        #[cfg(debug_assertions)]
        // '\n' in tail for the printed-line covering by '\r'
        eprintln!(
            "[DEBUG:walkdir_error!] {}:{}:{}\n",
            file!(),
            line!(),
            column!()
        );
        JustOneError::WalkdirError($err)
    }};
}

impl fmt::Display for JustOneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JustOneError::IOError { files, error } => {
                match files.len() {
                    0 => {}
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
        }
    }
}

impl Error for JustOneError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            JustOneError::IOError { files: _, error } => Some(error),
            JustOneError::WalkdirError(e) => Some(e),
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
type SymlinkContent = PathBuf;
type SymlinkPath = PathBuf;
#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
struct SmallHash(u64);
#[derive(Debug, Hash, Eq, PartialEq, Copy, Clone)]
struct FullHash(u64);

impl Default for JustOne {
    fn default() -> Self {
        let ignore_symlink = IGNORE_SYMLINK_DEFAULT;
        let follow_links = if ignore_symlink {
            false
        } else {
            FOLLOW_LINKS_DEFAULT
        };
        JustOne {
            hasher_creator: Box::new(|| Box::new(XxHash64::with_seed(XXHASH_SEED_DEFAULT))),
            strict_level: StrictLevel::default(),
            follow_links,
            ignore_error: IGNORE_ERROR_DEFAULT,
            ignored_files: Vec::new(),
            ignore_symlink,
            file_info: Vec::new(),
            file_index: HashMap::new(),
            size_dict: HashMap::new(),
            small_hash_dict: HashMap::new(),
            full_hash_dict: HashMap::new(),
            symlink_hash_dict: HashMap::new(),
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

    pub fn update(&mut self, dir: impl AsRef<Path>) -> Result<&mut Self> {
        self.update_directory(dir)?;

        Ok(self)
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<&Path>>> {
        let duplicate_files = match self.strict_level {
            StrictLevel::Common => self.duplicates_common()?,
            StrictLevel::Shallow => self.duplicates_strict(true)?,
            StrictLevel::ByteByByte => self.duplicates_strict(false)?,
        };
        if !self.ignore_symlink && !self.follow_links {
            let duplicate_symlinks = self.duplicates_symlink();
            Ok([duplicate_files, duplicate_symlinks].concat())
        } else {
            Ok(duplicate_files)
        }
    }

    fn duplicates_common(&self) -> Result<Vec<Vec<&Path>>> {
        Ok(self
            .full_hash_dict
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|(_, file_index_set)| {
                file_index_set
                    .iter()
                    .map(|file_index| self.get_file_path_by_index(*file_index))
                    .collect()
            })
            .collect())
    }

    fn duplicates_strict(&self, shallow: bool) -> Result<Vec<Vec<&Path>>> {
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

    fn duplicates_symlink(&self) -> Vec<Vec<&Path>> {
        self.symlink_hash_dict
            .iter()
            .filter(|(_, v)| v.len() > 1)
            .map(|(_, symlink_set)| symlink_set.iter().map(|p| p.as_ref()).collect())
            .collect()
    }

    fn update_directory(&mut self, dir: impl AsRef<Path>) -> Result<HashSet<FileIndex>> {
        let mut entries = Vec::new();
        for entry in WalkDir::new(dir).follow_links(self.follow_links) {
            let entry = match entry {
                Ok(val) => val,
                Err(e) if self.ignore_error => {
                    if let Some(path) = e.path() {
                        self.ignored_files.push(path.to_owned());
                    }
                    continue;
                }
                Err(e) => return Err(walkdir_error!(e)),
            };

            if !self.ignore_symlink && entry.path_is_symlink() {
                // deal with symlink
                match self.update_symlink(&entry) {
                    Ok(()) => {}
                    Err(_e) if self.ignore_error => {
                        self.ignored_files.push(entry.path().to_owned());
                        continue;
                    }
                    Err(e) => return Err(io_error!(e)),
                };
            } else if entry.file_type().is_file() {
                // deal with regular file
                entries.push(entry);
            }
        }
        // Processing symlinks separately, so all the files in entries are regular file
        self.update_regular_files(entries)
    }

    /// Processing symbolic links separately
    fn update_symlink(&mut self, symlink: &DirEntry) -> io::Result<()> {
        let key = fs::read_link(symlink.path())?;
        self.symlink_hash_dict
            .entry(key)
            .or_insert_with(|| HashSet::new())
            .insert(symlink.path().to_owned());
        Ok(())
    }

    fn update_regular_files<T>(&mut self, entries: T) -> Result<HashSet<FileIndex>>
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
                Err(_) if self.ignore_error => {
                    self.ignored_files
                        .push(self.file_info.get(file_index).unwrap().path.clone());
                    continue;
                }
                Err(e) => return Err(e),
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
                Err(_) if self.ignore_error => {
                    self.ignored_files
                        .push(self.file_info.get(file_index).unwrap().path.clone());
                    continue;
                }
                Err(e) => return Err(e),
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
        self.file_index.get(path).copied().unwrap_or_else(|| {
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
        })
    }

    fn get_file_path_by_index(&self, file_index: FileIndex) -> &Path {
        &self.file_info.get(file_index).unwrap().path
    }

    fn merge_size_dict(&mut self, size_dict_temp: SizeDict) -> Vec<(FileSize, FileIndex)> {
        let mut merged: Vec<(FileSize, FileIndex)> = Vec::new();
        for (file_size, file_index_set_temp) in size_dict_temp {
            self.size_dict
                .entry(file_size)
                .or_insert_with(|| HashSet::new());
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
            self.small_hash_dict
                .entry(file_size_and_small_hash)
                .or_insert_with(|| HashSet::new());
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
            self.full_hash_dict
                .entry(full_hash)
                .or_insert_with(|| HashSet::new());
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

    fn get_small_hash(&mut self, file_index: FileIndex) -> Result<SmallHash> {
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

    fn get_full_hash(&mut self, file_index: FileIndex) -> Result<FullHash> {
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

fn get_small_hash(f: &mut dyn io::Read, mut hasher: Box<dyn Hasher>) -> io::Result<SmallHash> {
    let mut buffer = [0; SMALL_HASH_CHUNK_SIZE];
    let read_size = f.read(&mut buffer)?;
    hasher.write(&buffer[..read_size]);
    Ok(SmallHash(hasher.finish()))
}

fn get_full_hash(f: &mut dyn io::Read, mut hasher: Box<dyn Hasher>) -> io::Result<FullHash> {
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

fn file_cmp(file_a: impl AsRef<Path>, file_b: impl AsRef<Path>, shallow: bool) -> Result<bool> {
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
