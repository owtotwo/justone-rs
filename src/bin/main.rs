use std::path::Path;
use std::error::Error;
use justone::{self, JustOne};

fn main() -> Result<(), Box<dyn Error>>{
    println!("Hello, justone!");
    let mut jo = JustOne::new();
    let dir_path_1 = Path::new("test_data");
    let dir_path_2 = Path::new("D:\\test_dup_data");
    jo.update(&dir_path_1)?.update(&dir_path_2)?;
    let dups: Result<Vec<Vec<Box<Path>>>, Box<dyn Error>> = jo.duplicates();

    match dups {
        Ok(dups) => {
            for (i, dup) in dups.iter().enumerate() {
                println!("[{}] Duplicate found:", i);
                for path in dup {
                    println!(" - {:?}", path);
                }
            }
        },
        Err(_) => {
            eprintln!("Something Wrong...");
        },
    };

    Ok(())
}
