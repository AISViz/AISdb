#![allow(dead_code)]

///
/// AISDB Rust CLI
/// Create new SQLite databases from AIS data
///

pub const HELP: &str = "\
AISDB

USAGE:
  cargo run --release -- [DBPATH] [RAWDATA_DIR]

FLAGS:
  -h, --help      Prints this message

OPTIONS:
  --dbpath        SQLite database path
  --rawdata_dir   Path to .nm4 data files
  --start         Optionally skip the first N files     [default=0]
  --end           Optionally skip files after index N   [default=usize::MAX]

";

use std::fs::read_dir;
use std::path::Path;

use nmea_parser::ParsedMessage;

use crate::decode::VesselData;

#[derive(Debug)]
pub struct AppArgs {
    pub dbpath: std::path::PathBuf,
    //pub rawdata_dir: std::path::Path,
    pub rawdata_dir: String,
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
            .unwrap_or(Path::new(":memory:").to_path_buf()),
        rawdata_dir: pargs
            .opt_value_from_str("--rawdata_dir")
            .unwrap()
            .unwrap_or("testdata/".to_string()),
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
pub fn glob_dir(dirname: &str, matching: &str, skip: usize) -> Option<Vec<String>> {
    println!("{}", dirname);
    let mut fnames = read_dir(dirname)
        .expect("glob dir")
        .map(|f| f.unwrap().path().display().to_string())
        .filter(|f| &f[f.len() - matching.chars().count()..] == matching)
        .collect::<Vec<String>>()
        .to_vec();
    fnames.sort();
    Some(fnames[skip..].to_vec())
}

/// interpret VesselData struct as row data and perform a matrix transpose.
/// returns column vectors
pub fn msgs_transpose(
    msgs: Vec<VesselData>,
) -> Option<(
    Vec<u32>,
    Vec<i32>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<bool>,
    Vec<u8>,
)> {
    let rows = msgs
        .iter()
        .map(|m| (m.payload.as_ref().unwrap(), m.epoch))
        .map(|t| match t {
            (ParsedMessage::VesselDynamicData(m), e) => (
                m.mmsi,
                e.unwrap(),
                m.longitude.unwrap(),
                m.latitude.unwrap(),
                m.rot.unwrap(),
                m.sog_knots.unwrap(),
                m.cog.unwrap(),
                m.heading_true.unwrap(),
                m.special_manoeuvre.unwrap(),
                m.timestamp_seconds,
            ),
            _ => {
                panic!("this should not happen ideally, otherwise uncomment next line");
                //(0, 0, 0., 0., 0., 0., 0., 0., false, 0)
            }
        })
        .collect::<Vec<(u32, i32, f64, f64, f64, f64, f64, f64, bool, u8)>>();

    let mut v0: Vec<u32> = Vec::new();
    let mut v1: Vec<i32> = Vec::new();
    let mut v2: Vec<f64> = Vec::new();
    let mut v3: Vec<f64> = Vec::new();
    let mut v4: Vec<f64> = Vec::new();
    let mut v5: Vec<f64> = Vec::new();
    let mut v6: Vec<f64> = Vec::new();
    let mut v7: Vec<f64> = Vec::new();
    let mut v8: Vec<bool> = Vec::new();
    let mut v9: Vec<u8> = Vec::new();

    rows.into_iter().for_each(|r| {
        v0.push(r.0);
        v1.push(r.1);
        v2.push(r.2);
        v3.push(r.3);
        v4.push(r.4);
        v5.push(r.5);
        v6.push(r.6);
        v7.push(r.7);
        v8.push(r.8);
        v9.push(r.9);
    });

    Some((v0, v1, v2, v3, v4, v5, v6, v7, v8, v9))
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_glob_dir() {
        let _ = glob_dir("src", "rs", 0);
    }
}
