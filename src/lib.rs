//! Rust exports for python API
use std::cmp::max;
use std::path::PathBuf;
use std::process::exit;
use std::thread::sleep;
use std::time::Duration;

use geo::algorithm::simplifyvw::SimplifyVwIdx;
use geo::point;
use geo::HaversineDistance;
use geo_types::{Coord, LineString};
use nmea_parser::NmeaParser;
use pyo3::ffi::PyErr_CheckSignals;
use pyo3::prelude::*;

use aisdb_lib::csvreader::decodemsgs_ee_csv;
use aisdb_lib::decode::decode_insert_msgs;
use aisdb_receiver::{start_receiver, ReceiverArgs};

macro_rules! zip {
    ($x: expr) => ($x);
    ($x: expr, $($y: expr), +) => (
        $x.iter().zip(
            zip!($($y), +))
        )
}

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
#[pyfunction]
pub fn haversine(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let p1 = point!(x: x1, y: y1);
    let p2 = point!(x: x2, y: y2);
    p1.haversine_distance(&p2)
}

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
///     verbose (bool)
///         enables logging
///
/// returns:
///     None
///
#[pyfunction]
pub fn decoder(dbpath: &str, files: Vec<&str>, source: &str, verbose: bool) {
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
        if f.to_str().unwrap().contains(".nm4")
            || f.to_str().unwrap().contains(".NM4")
            || f.to_str().unwrap().contains(".RX")
            || f.to_str().unwrap().contains(".rx")
            || f.to_str().unwrap().contains(".TXT")
            || f.to_str().unwrap().contains(".txt")
        {
            parser = decode_insert_msgs(d, f, source, parser, verbose).expect("decoding NM4");
        } else if f.to_str().unwrap().contains(".csv") || f.to_str().unwrap().contains(".CSV") {
            decodemsgs_ee_csv(d, f, source, verbose).expect("decoding CSV");
        } else {
            panic!("unknown file extension {:?}", d);
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
        .map(|(xx, yy)| Coord { x: *xx, y: *yy })
        .collect();
    let line = LineString(coords).simplifyvw_idx(&precision);
    line.into_iter().collect::<Vec<usize>>()
}

/// Assigns a score for likelihood of two points being part of a sequential
/// vessel trajectory. A hard cutoff will be applied at distance_threshold,
/// after which all scores will be set to -1.
///
/// args:
///     x1 (float)
///         longitude of coordinate pair 1
///     y1 (float)
///         latitude of coordinate pair 1
///     t1 (float)
///         Timestamp for coordinate pair 1 in epoch seconds
///     x2 (float)
///         longitude of coordinate pair 2
///     y2 (float)
///         latitude of coordinate pair 2
///     t2 (float)
///         Timestamp for coordinate pair 2 in epoch seconds
///     speed_threshold (float)
///         Tracks will be segmented between points where computed
///         speed values exceed this threshold. Segmented tracks will
///         be scored for reconnection. Measured in knots
///     distance_threshold (float)
///         Used as a numerator when determining score; this value
///         is divided by the distance between xy1 and xy2.
///         If the distance between xy1 and xy2 exceeds this value,
///         the score will be set to -1. Measured in meters
///
/// returns:
///     score (float: f64)
#[pyfunction]
pub fn encoder_score_fcn(
    x1: f64,
    y1: f64,
    t1: i32,
    x2: f64,
    y2: f64,
    t2: i32,
    speed_thresh: f64,
    dist_thresh: f64,
) -> f64 {
    // great circle distance between coordinate pairs (meters)
    let mut dm = haversine(x1, y1, x2, y2);
    if dm < 1.0 {
        dm = 1.0;
    }
    // elapsed time (seconds)
    let dt = max(t2 - t1, 10) as f64;
    // computed speed (knots)
    let ds = (dm / dt) * 1.9438444924406;

    if ds < speed_thresh && dm < dist_thresh * 2.0 {
        dist_thresh / ds
    } else {
        -1.0
    }
}

/// Vectorized implementation of binary search for fast array indexing.
/// In out-of-bounds or missing value cases, the nearest search index
/// will be returned
///
/// args:
///     arr (Vec<f64>)
///         sorted array of values to be indexed. values can be sorted
///         either by ascending or descending
///     search (Vec<f64>)
///         values to be searched within ``arr``
///
/// returns:
///     indexes (Vec<i32>)
///
#[pyfunction]
pub fn binarysearch_vector(mut arr: Vec<f64>, search: Vec<f64>) -> Vec<i32> {
    let descending;
    if arr[0] > arr[arr.len() - 1] {
        descending = true;
        arr.reverse();
    } else {
        descending = false;
    }

    search
        .into_iter()
        .map(|s| arr.binary_search_by(|v| v.partial_cmp(&s).expect("Couldn't compare values")))
        .map(|idx| match idx {
            Ok(i) => i as i32,
            Err(i) => {
                if (i as i32) < 0 {
                    0
                } else if i >= (arr.len()) {
                    (arr.len() - 1) as i32
                } else {
                    i as i32
                }
            }
        })
        .map(|idx| {
            if !descending {
                idx
            } else {
                (arr.len() - 1) as i32 - idx
            }
        })
        .collect::<Vec<i32>>()
}

/// Receive raw AIS data from an upstream UDP data source, parse the data into
/// JSON format, and create a websocket listener to send parsed results downstream.
/// If dbpath is given, parsed data will be stored in an SQLite database.
///
/// args:
///     dbpath (Option<String>)
///         If given, raw messages will be parsed and stored in an SQLite database at this location
///     udp_listen_addr (String)
///         UDP port to listen for incoming AIS data streams e.g. "0.0.0.0:9921" or "[::]:9921"
///     tcp_listen_addr (String)
///         if not None, a thread will be spawned to forward TCP connections to
///         incoming port ``udp_listen_addr``
///     multicast_addr (String)
///         Raw UDP messages will be parsed and then routed to TCP socket listeners via this channel.
///     multicast_rebroadcast (Option<String>)
///         Optionally pass a rebroadcast address where raw data will be filtered
///         and rebroadcasted to this channel for e.g. forwarding to downstream
///         networks
///     tcp_output_addr (String)
///         TCP port to listen for websocket clients to send parsed data in JSON format
///     dynamic_msg_bufsize (Option<usize>)
///         Number of positional messages to keep before inserting into the database.
///         Defaults to 256 if none is given
///     static_msg_bufsize (Option<usize>)
///         Number of static messages to keep before inserting into database.
///         Defaults to 64
///     tee (bool)
///         If True, raw input will be copied to stdout
#[pyfunction]
pub fn receiver(
    dbpath: Option<String>,
    tcp_connect_addr: Option<String>,
    tcp_listen_addr: Option<String>,
    udp_listen_addr: String,
    multicast_addr_parsed: Option<String>,
    multicast_addr_raw: Option<String>,
    tcp_output_addr: Option<String>,
    udp_output_addr: Option<String>,
    dynamic_msg_bufsize: Option<usize>,
    static_msg_bufsize: Option<usize>,
    tee: bool,
) {
    let threads = start_receiver(ReceiverArgs {
        dbpath: dbpath.map(PathBuf::from),
        tcp_connect_addr,
        tcp_listen_addr,
        udp_listen_addr,
        multicast_addr_parsed,
        multicast_addr_rawdata: multicast_addr_raw,
        tcp_output_addr,
        udp_output_addr,
        dynamic_msg_bufsize,
        static_msg_bufsize,
        tee,
    });
    unsafe {
        while threads
            .iter()
            .map(|t| t.is_finished())
            .filter(|b| !(*b))
            .count()
            > 0
        {
            let signal = PyErr_CheckSignals();
            if signal != 0 {
                eprintln!("exiting...");
                exit(signal);
            }
            sleep(Duration::from_millis(500));
        }
    }
}

/// Functions imported from Rust
#[pymodule]
pub fn aisdb(_py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(haversine))?;
    module.add_wrapped(wrap_pyfunction!(decoder))?;
    module.add_wrapped(wrap_pyfunction!(simplify_linestring_idx))?;
    module.add_wrapped(wrap_pyfunction!(encoder_score_fcn))?;
    module.add_wrapped(wrap_pyfunction!(binarysearch_vector))?;
    module.add_wrapped(wrap_pyfunction!(receiver))?;
    Ok(())
}
