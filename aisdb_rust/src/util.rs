#![allow(dead_code)]

///
/// AISDB Rust CLI
/// Create new SQLite databases from AIS data
///

pub const HELP: &str = "
AISDB
  convert AIS data in .nm4 format to an SQLite database containing
  vessel position reports and static data reports
  (message types 1, 2, 3, 5, 18, 24, 27)

USAGE:
  aisdb --dbpath DBPATH ... [OPTIONS]

ARGS:
  --dbpath        SQLite database path

OPTIONS:
  -h, --help      Prints this message
  --file          Path to .nm4 file. Can be repeated multiple times
  --rawdata_dir   Path to .nm4 data directory

";

use std::fs::read_dir;
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, Utc};

pub struct AppArgs {
    pub dbpath: std::path::PathBuf,
    pub files: Vec<std::path::PathBuf>,
    pub rawdata_dir: Option<std::path::PathBuf>,
    pub start: usize,
    pub end: usize,
}

pub fn parse_path(s: &std::ffi::OsStr) -> Result<std::path::PathBuf, &'static str> {
    Ok(s.into())
}

/// collect --dbpath and --rawdata_dir args from command line
pub fn parse_args() -> Result<AppArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    if pargs.contains(["-h", "--help"]) || pargs.clone().finish().is_empty() {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let args = AppArgs {
        dbpath: pargs
            .opt_value_from_str("--dbpath")
            .unwrap()
            .unwrap_or_else(|| Path::new(":memory:").to_path_buf()),
        rawdata_dir: pargs
            .opt_value_from_os_str("--rawdata_dir", parse_path)
            .unwrap(),
        files: pargs.values_from_os_str("--file", parse_path).unwrap(),
        start: pargs
            .opt_value_from_fn("--start", str::parse)
            .unwrap()
            .unwrap_or(0),
        end: pargs
            .opt_value_from_fn("--end", str::parse)
            .unwrap()
            .unwrap_or(usize::MAX),
    };

    let remaining = pargs.finish();
    if !remaining.is_empty() {
        eprintln!("unused args {:?}", remaining);
    }
    Ok(args)
}

/// yields sorted vector of files in dirname with a matching file extension.
/// Optionally, skip the first N results
pub fn glob_dir(dirname: std::path::PathBuf, matching: &str) -> Option<Vec<String>> {
    println!("{:?}", dirname);
    let mut fnames = read_dir(dirname)
        .expect("glob dir")
        .map(|f| f.unwrap().path().display().to_string())
        .filter(|f| &f[f.len() - matching.chars().count()..] == matching)
        .collect::<Vec<String>>()
        .to_vec();
    fnames.sort();
    Some(fnames)
}

pub fn epoch_2_dt(e: i64) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(e, 0), Utc)
}

#[cfg(test)]

mod tests {
    use super::glob_dir;
    use crate::HELP;

    #[test]
    fn test_glob_dir() {
        let _ = glob_dir(std::path::PathBuf::from("src/"), "rs");
    }

    #[test]
    fn write_readme() {
        let txtfile = format!(
            "{}{}",
            "benchmarking rust for SQLite DB inserts\n\ncompiles to ./target/release/aisdb\n\n",
            HELP
        );
        let _ = std::fs::write("./readme.txt", txtfile);
    }
}
