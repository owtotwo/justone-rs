use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const TEST_DIR_NAME: &'static str = "test_justone";

fn get_test_dir_path() -> PathBuf {
    env::temp_dir().join(TEST_DIR_NAME)
}

/// Remove file, symlink or path(-r)
fn remove_path(p: impl AsRef<Path>) -> io::Result<()> {
    if p.as_ref().exists() {
        if p.as_ref().symlink_metadata()?.is_dir() {
            fs::remove_dir_all(&p)?;
        } else { // is file or symlink
            fs::remove_file(&p)?;
        }
    }
    Ok(())
}

// Create some files in temp-dir for tests
pub fn setup() -> io::Result<PathBuf> {
    let test_dir = get_test_dir_path();
    
    remove_path(&test_dir)?;
    fs::create_dir_all(&test_dir)?;

    let a = fs::File::create(test_dir.join("A"))?;

    Ok(test_dir)
}

// clean test dir
pub fn teardown() -> io::Result<()> {
    let test_dir = get_test_dir_path();

    remove_path(&test_dir)?;

    Ok(())
}