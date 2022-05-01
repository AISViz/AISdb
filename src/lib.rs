use geo::point;
use geo::prelude::*;
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

#[pyfunction]
pub fn haversine(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let p1 = point!(x: x1, y: y1);
    let p2 = point!(x: x2, y: y2);
    p1.haversine_distance(&p2)
}

#[pyfunction]
pub fn decode_native(dbpath: &str, files: Vec<&str>) {
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
            parser = decode_insert_msgs(&d, &f, parser).expect("decoding NM4");
        } else if f.to_str().unwrap().contains(&".csv") || f.to_str().unwrap().contains(&".CSV") {
            decodemsgs_ee_csv(&d, &f).expect("decoding CSV");
        } else {
            panic!("unknown file extension {:?}", &d);
        }
    }
}

#[pymodule]
#[allow(unused_variables)]
pub fn aisdb(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(haversine))?;
    module.add_wrapped(wrap_pyfunction!(decode_native))?;
    Ok(())
}

// pip/pac install maturin
// maturin develop
