use geo::algorithm::simplifyvw::SimplifyVwIdx;
use geo::point;
use geo::prelude::*;
use geo_types::{Coordinate, LineString};
use nmea_parser::NmeaParser;
use pyo3::prelude::*;

#[path = "csvreader.rs"]
pub mod csvreader;

#[path = "db.rs"]
pub mod db;

#[path = "decode.rs"]
pub mod decode;

#[path = "util.rs"]
pub mod util;

use csvreader::*;
use decode::*;

macro_rules! zip {
    ($x: expr) => ($x);
    ($x: expr, $($y: expr), +) => (
        $x.iter().zip(
            zip!($($y), +))
        )
}

#[pyfunction]
pub fn haversine(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let p1 = point!(x: x1, y: y1);
    let p2 = point!(x: x2, y: y2);
    p1.haversine_distance(&p2)
}

#[pyfunction]
pub fn decoder(dbpath: &str, files: Vec<&str>, source: &str) {
    // array tuples containing (dbpath, filepath)
    let mut path_arr = vec![];
    for file in files {
        path_arr.push((
            std::path::PathBuf::from(&dbpath),
            std::path::PathBuf::from(file),
        ));
    }

    // check file extensions and begin decode
    let mut parser = NmeaParser::new();
    for (d, f) in &path_arr {
        if f.to_str().unwrap().contains(&".nm4")
            || f.to_str().unwrap().contains(&".NM4")
            || f.to_str().unwrap().contains(&".RX")
            || f.to_str().unwrap().contains(&".rx")
            || f.to_str().unwrap().contains(&".TXT")
            || f.to_str().unwrap().contains(&".txt")
        {
            parser = decode_insert_msgs(&d, &f, &source, parser).expect("decoding NM4");
        } else if f.to_str().unwrap().contains(&".csv") || f.to_str().unwrap().contains(&".CSV") {
            decodemsgs_ee_csv(&d, &f, &source).expect("decoding CSV");
        } else {
            panic!("unknown file extension {:?}", &d);
        }
    }
}

#[pyfunction]
pub fn simplify_linestring_idx(x: Vec<f32>, y: Vec<f32>, precision: f32) -> Vec<usize> {
    let coords = zip!(&x, &y)
        .map(|(xx, yy)| Coordinate { x: *xx, y: *yy })
        .collect();
    let line = LineString(coords).simplifyvw_idx(&precision);
    line.into_iter().collect::<Vec<usize>>()
}

#[pymodule]
#[allow(unused_variables)]
pub fn aisdb(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(haversine))?;
    module.add_wrapped(wrap_pyfunction!(decoder))?;
    module.add_wrapped(wrap_pyfunction!(simplify_linestring_idx))?;
    Ok(())
}
