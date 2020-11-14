use std::path::Path;
use std::error::Error;
use justone::{self, JustOne};

fn main() -> Result<(), Box<dyn Error>>{
    println!("Hello, justone!");
    let mut jo = JustOne::new();
    jo.update("test_data")?;
    jo.update("D:\\test_dup_data")?;
    let dups: Result<Vec<Vec<&Path>>, Box<dyn Error>> = jo.duplicates();

    match dups {
        Ok(dups) => {
            for (i, dup) in dups.iter().enumerate() {
                println!("[{}] Duplicate found:", i + 1);
                for path in dup {
                    println!(" - {}", path.display());
                }
            }
        },
        Err(e) => {
            eprintln!("Something Wrong...{:?}", e);
        },
    };

    Ok(())
}
