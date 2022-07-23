//! Rust exports for python API

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

#[path = "load_geotiff.rs"]
pub mod load_geotiff;

#[path = "util.rs"]
pub mod util;

use csvreader::*;
use decode::*;
//use load_geotiff::load_pixel;

macro_rules! zip {
    ($x: expr) => ($x);
    ($x: expr, $($y: expr), +) => (
        $x.iter().zip(
            zip!($($y), +))
        )
}

#[pyfunction]
/// fast great circle distance
///
/// args:
///     x1 (float64)
///         longitude of coordinate pair 1
///     y1 (float64)
///         latitude of coordinate pair 1
///     x2 (float64)
///         longitude of coordinate pair 2
///     y2 (float64)
///         latitude of coordinate pair 2
///
/// returns:
///     distance in metres (float64)
///
pub fn haversine(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let p1 = point!(x: x1, y: y1);
    let p2 = point!(x: x2, y: y2);
    p1.haversine_distance(&p2)
}

#[pyfunction]
/// Parse NMEA-formatted strings, and create SQLite database
/// from raw AIS transmissions
///
/// args:
///     dbpath (&str)
///         output database file
///     files (array of &str)
///         array of .nm4 raw data filepath strings
///     source (&str)
///         data source text. Will be used as a primary key index in database
///
/// returns:
///     None
///
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

/// linear curve decimation using visvalingam-whyatt algorithm.
///
/// args:
///     x (array of float32)
///         longitudes
///     y (array of float32)
///         latitudes
///     precision (float32)
///         coordinates will be rounded to the nearest value.
///         e.g. 0.01 for decimation to within a few km radius
///
/// returns:
///     Vec<usize>
///         Array of indices along (x,y)
///
#[pyfunction]
pub fn simplify_linestring_idx(x: Vec<f32>, y: Vec<f32>, precision: f32) -> Vec<usize> {
    let coords = zip!(&x, &y)
        .map(|(xx, yy)| Coordinate { x: *xx, y: *yy })
        .collect();
    let line = LineString(coords).simplifyvw_idx(&precision);
    line.into_iter().collect::<Vec<usize>>()
}

/*
#[pyfunction]
pub fn load_geotiff_pixel(lon: usize, lat: usize, filepath: &str) -> usize {
load_pixel(lon, lat, &filepath)
}
*/

/// Functions imported from Rust
#[pymodule]
#[allow(unused_variables)]
pub fn aisdb(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(haversine))?;
    module.add_wrapped(wrap_pyfunction!(decoder))?;
    module.add_wrapped(wrap_pyfunction!(simplify_linestring_idx))?;
    //module.add_wrapped(wrap_pyfunction!(load_geotiff_pixel))?;
    Ok(())
}
