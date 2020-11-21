use std::error::Error;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use clap::{App, Arg};
use justone::{self, JustOne, StrictLevel, default_hasher_creator};

const APP_NAME: &'static str = env!("CARGO_PKG_NAME");
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const AUTHORS : &'static str = env!("CARGO_PKG_AUTHORS");
const BIN_NAME: &'static str = env!("CARGO_BIN_NAME");
const DESCRIPTION: &'static str = env!("CARGO_PKG_DESCRIPTION");

fn main() {
    let matches = App::new(APP_NAME)
        .version(VERSION)
        .author(AUTHORS)
        .about(DESCRIPTION)
        .bin_name(BIN_NAME)
        .arg(Arg::with_name("folder")
            .help("The folder where you want to find duplicate files")
            .value_name("FOLDER")
            .required(true)
            .multiple(true)
            .takes_value(true)
            .empty_values(false)
            .index(1))
        .arg(Arg::with_name("strict")
            .short("s")
            .long("strict")
            .multiple(true)
            .takes_value(false)
            .help("Strict level for file comparison")
            .long_help("[0][default] Based on hash comparison.\n\
                [1][-s] Shallow comparison based on file stat, and byte comparison when inconsistent, to prevent hash collision.\n\
                [2][-ss] Strictly compare byte by byte to prevent file stat and hash collision.\n"))
        .arg(Arg::with_name("ignore-error")
            .short("i")
            .long("ignore-error")
            .help("Ignore error such as PermissionError or FileNotExisted")
            .takes_value(false)
            .required(false)
            .multiple(false))
        .arg(Arg::with_name("time")
            .short("t")
            .long("time")
            .help("Show total time consumption")
            .takes_value(false)
            .required(false)
            .multiple(false))
        .arg(Arg::with_name("output")
            .short("o")
            .long("output")
            .help("Output result to file")
            .takes_value(true)
            .required(false)
            .multiple(false))
        .get_matches();

    let folders: Vec<_> = matches.values_of("folder").unwrap().collect();
    let strict_level = matches.occurrences_of("strict");
    let ignore_error = matches.is_present("ignore-error");
    let time_it = matches.is_present("time");
    let output = matches.value_of("output");

    let strict_level = match strict_level {
        0 => StrictLevel::Common,
        1 => StrictLevel::Shallow,
        2 => StrictLevel::ByteByByte,
        x @ _ => {
            eprintln!(
                "{} is not a valid level for file comparison strict level. (need -s, -ss or unset)",
                x
            );
            std::process::exit(1);
        }
    };

    let output: Box<dyn Write> = if let Some(path) = output {
        match File::create(path) {
            Ok(f) => Box::new(f),
            Err(e) => {
                eprintln!("Error: {}", e);
                eprintln!(
                    "Because of {:?}, failed to create a output file {} for writing result.",
                    e.kind(),
                    path
                );
                std::process::exit(1);
            }
        }
    } else {
        Box::new(io::stdout())
    };

    if let Err(e) = print_duplicates(folders, output, strict_level, ignore_error, time_it) {
        eprintln!(""); // newline
        eprintln!("Error: {}", e);
        std::process::exit(1);
    };
}

fn print_duplicates(
    folders: Vec<impl AsRef<Path>>,
    mut output: Box<dyn Write>,
    strict_level: StrictLevel,
    ignore_error: bool,
    time_it: bool,
) -> Result<(), Box<dyn Error>> {
    let mut jo = JustOne::with_full_config(default_hasher_creator(), strict_level, ignore_error);

    let start = Instant::now();

    for folder in folders {
        jo.update(folder)?;
    }

    let dups = jo.duplicates()?;

    let time_waste = start.elapsed();

    for (i, dup) in dups.iter().enumerate() {
        if i != 0 {
            writeln!(&mut output, "")?;
        }
        writeln!(&mut output, "[{}] Duplicate found:", i + 1)?;
        for path in dup {
            writeln!(&mut output, " - {}", path.display())?;
        }
    }

    if time_it {
        println!("Time Waste: {:?}s", time_waste);
    }

    Ok(())
}
