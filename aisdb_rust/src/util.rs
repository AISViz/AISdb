#![allow(dead_code)]

use std::env;
use std::fs::read_dir;

use nmea_parser::ParsedMessage;

use crate::decode::VesselData;

/// yields sorted vector of files in dirname with a matching file extension.
/// Optionally, skip the first N results
pub fn glob_dir(dirname: &str, matching: &str, skip: usize) -> Option<Vec<String>> {
    //    let mut fpaths = read_dir("testdata/")
    //        .unwrap()
    //        .map(|f| f.unwrap().path().display().to_string())
    //        .collect::<Vec<String>>();
    //    fpaths.sort();
    /*
       let files = read_dir(dirname).unwrap();
       let mut fnames: Vec<String> = [].to_vec();
       for f in files {
    //let path_str = f.ok()?.path().to_str().unwrap();
    //match path_str.rsplit_once(".") {
    match f.ok()?.path().to_str().unwrap().rsplit_once(".") {
    Some((pattern, "nm4")) | Some((pattern, "NM4")) => {
    let fname = format!("{}.{}", pattern, matching);
    fnames.push(fname);
    }
    q => {
    println!("skipping path {}", q.unwrap().0);
    continue;
    }
    }
    }
    */
    let mut fnames = read_dir(dirname)
        .unwrap()
        .map(|f| f.unwrap().path().display().to_string())
        .filter(|f| &f[f.len() - matching.chars().count()..] == matching)
        .collect::<Vec<String>>()
        .to_vec();
    fnames.sort();
    Some(fnames[skip..].to_vec())
}

/// collect --dbpath and --rawdata_dir args from command line
pub fn parse_args() -> (String, String) {
    let mut dbpath = "testdata/ais.db";
    let mut rawdata_dir = "testdata/";
    let args: Vec<String> = env::args().collect();
    for arg in &args {
        if arg.contains("--dbpath") {
            dbpath = arg.rsplit_once("=").unwrap().1;
        } else if arg.contains("--rawdata_dir") {
            rawdata_dir = arg.rsplit_once("=").unwrap().1;
        }
    }
    (dbpath.to_string(), rawdata_dir.to_string())
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
    /*

                   let qry = query(&sql)
                   .bind(&v0.into())
                   .bind(&v1.into())
                   .bind(&v2.into())
                   .bind(&v3.into())
                   .bind(&v4.into())
                   .bind(&v5.into())
                   .bind(&v6.into())
                   .bind(&v7.into())
                   .bind(&v8.into())
                   .bind(&v9.into());

    */
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_glob_dir() {
        let _ = glob_dir("src", "rs", 0);
    }
}
