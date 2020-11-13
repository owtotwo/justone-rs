use std::path::Path;
use std::error::Error;
use walkdir::WalkDir;

pub struct JustOne {

}

impl JustOne {
    pub fn new() -> Self {
        JustOne {}
    }

    pub fn update(&mut self, dir: &Path) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    pub fn duplicates(&self) -> Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> {
        Ok(vec![])
    }
}