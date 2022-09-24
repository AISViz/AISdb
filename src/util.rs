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
  --source        Datasource name

OPTIONS:
  -h, --help      Prints this message
  --file          Path to .nm4 file. Can be repeated multiple times
  --rawdata_dir   Path to .nm4 data directory

";

use std::fs::read_dir;

use chrono::{DateTime, NaiveDateTime, Utc};

/// yields sorted vector of files in dirname with a matching file extension.
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
    use super::HELP;

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
        let _ = std::fs::write("./src/readme.txt", txtfile);
    }
}
