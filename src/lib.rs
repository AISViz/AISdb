//! Rust exports for python API
use std::cmp::{max, min};
use std::fs::metadata;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::thread::{available_parallelism, sleep};
use std::time::Duration;

use futures::executor::ThreadPool;
use geo::{point, HaversineDistance, SimplifyVwIdx};
use geo_types::{Coord, LineString};
use nmea_parser::NmeaParser;
use pyo3::{pyfunction, pymodule, types::PyModule, wrap_pyfunction, PyResult, Python};
use sysinfo::{RefreshKind, System, SystemExt};

use aisdb_lib::csvreader::{postgres_decodemsgs_ee_csv, sqlite_decodemsgs_ee_csv};
use aisdb_lib::decode::{postgres_decode_insert_msgs, sqlite_decode_insert_msgs};
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

/// Parse NMEA-formatted strings, and create databases
/// from raw AIS transmissions
///
/// args:
///     dbpath (str)
///         Output SQLite database path. Set this to an empty string to only use Postgres
///     psql_conn_string (str)
///         Postgres database connection string. Set this to an empty string to only use SQLite
///     files (array of str)
///         array of .nm4 raw data filepath strings
///     source (str)
///         data source text. Will be used as a primary key index in database
///     verbose (bool)
///         enables logging
///
/// returns:
///     None
///
#[pyfunction]
#[pyo3(text_signature = "(dbpath, psql_conn_string, files, source, verbose)")]
pub fn decoder(
    dbpath: PathBuf,
    psql_conn_string: String,
    files: Vec<PathBuf>,
    source: String,
    verbose: bool,
    py: Python,
) -> Vec<PathBuf> {
    // tuples containing (dbpath, filepath)
    let mut path_arr: Vec<(PathBuf, PathBuf)> = Vec::new();
    for file in &files {
        path_arr.push((dbpath.clone(), file.to_path_buf()));
    }

    // check average file size for memory allocations and multiply by 2
    let mut bytesize: u64 = 0;
    for file in &files {
        bytesize += metadata(file).expect("getting file size").len();
    }
    bytesize /= files.len() as u64;
    bytesize *= 2;

    // keep track of memory use before spawning new parallel workers
    let mut sys = System::new_with_specifics(RefreshKind::new().with_memory());
    sys.refresh_memory();

    // reserve atleast 3.5GB of available memory for each worker thread,
    // up to a maximum of one worker per CPU
    let worker_count = min(
        min(
            max(1, (sys.available_memory() - bytesize) / bytesize),
            min(32, available_parallelism().expect("CPU count").get() as u64),
        ),
        files.len() as u64,
    );
    let pool = ThreadPool::builder()
        .pool_size(worker_count as usize)
        .name_prefix("aisdb-decode-")
        .create()
        .expect("creating pool");

    // when each worker is done they will send "true" to the receiver
    let (sender, receiver) = channel::<Result<PathBuf, PathBuf>>();
    let mut completed = Vec::new();
    let mut errored = Vec::new();

    fn update_done_files(
        completed: &mut Vec<PathBuf>,
        errored: &mut Vec<PathBuf>,
        next: Result<PathBuf, PathBuf>,
    ) {
        match next {
            Ok(p) => completed.push(p),
            Err(p) => errored.push(p),
        }
    }

    // count workers in progress
    let mut in_process: u64 = 0;

    println!(
        "Memory: {:.2}GB remaining.  CPUs: {}.  Average file size: {:.2}MB  Spawning {} workers",
        sys.available_memory() as f64 * 1e-9_f64,
        available_parallelism().expect("CPU count"),
        bytesize as f64 * 1e-6_f64,
        worker_count
    );

    // check file extensions and begin decode
    let mut parser = NmeaParser::new();
    for (d, f) in path_arr.iter() {
        let f = f.clone();
        let source = source.clone();
        let psql_conn_string = psql_conn_string.clone();

        py.check_signals().expect("checking signals");
        sleep(System::MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_memory();

        // wait until the system has some available memory
        while (in_process * bytesize > sys.total_memory() - bytesize
            || sys.available_memory() < bytesize)
            && in_process != 0
        {
            sleep(System::MINIMUM_CPU_UPDATE_INTERVAL + Duration::from_millis(50));
            // check if keyboardinterrupt was sent
            py.check_signals()
                .expect("Decoder interrupted while spawning workers");
            // check if worker completed a file
            match receiver.try_recv() {
                Ok(r) => {
                    update_done_files(&mut completed, &mut errored, r);
                    in_process -= 1;
                }
                Err(_r) => {
                    sleep(std::time::Duration::from_millis(100));
                }
            }

            sys.refresh_memory();
        }
        if verbose {
            println!("processing {}", f.display());
        }

        match f.extension() {
            Some(ext_os_str) => match ext_os_str.to_str() {
                Some("nm4") | Some("NM4") | Some("nmea") | Some("NMEA") | Some("rx")
                | Some("txt") | Some("RX") | Some("TXT") => {
                    if !dbpath.to_str().unwrap().is_empty() {
                        parser = sqlite_decode_insert_msgs(
                            d.to_path_buf(),
                            f.clone(),
                            &source,
                            parser,
                            verbose,
                        )
                        .expect("decoding NM4");
                    }
                    if !psql_conn_string.is_empty() {
                        let sender = sender.clone();
                        let future = async move {
                            let parser = NmeaParser::new();
                            match postgres_decode_insert_msgs(
                                &psql_conn_string,
                                f.clone(),
                                &source,
                                parser,
                                verbose,
                            ) {
                                Err(_) => {
                                    sender
                                        .send(Err(f))
                                        .expect("sending errored filepath from worker");
                                }
                                Ok(_) => sender
                                    .send(Ok(f))
                                    .expect("sending completed filepath from worker"),
                            };
                        };
                        pool.spawn_ok(future);
                        in_process += 1;
                    }
                }
                Some("csv") | Some("CSV") => {
                    if dbpath != PathBuf::from("") {
                        sqlite_decodemsgs_ee_csv(d.to_path_buf(), f.clone(), &source, verbose)
                            .expect("decoding CSV");
                    }
                    if !psql_conn_string.is_empty() {
                        let sender = sender.clone();
                        let future = async move {
                            match postgres_decodemsgs_ee_csv(
                                &psql_conn_string,
                                &f,
                                &source,
                                verbose,
                            ) {
                                Err(e) => {
                                    eprintln!("CSV decoder error: {}\n", e);
                                    sender.send(Err(f.clone())).unwrap_or_else(|e| {
                                        eprintln!(
                                            "sending errored CSV filepath from worker {}\n{}",
                                            f.display(),
                                            e
                                        )
                                    });
                                }
                                Ok(_) => sender.send(Ok(f.clone())).unwrap_or_else(|e| {
                                    eprintln!(
                                        "sending completed CSV filepath from worker {}\n{}",
                                        f.display(),
                                        e
                                    )
                                }),
                            };
                        };
                        pool.spawn_ok(future);
                        in_process += 1;
                    }
                }
                _ => {
                    panic!("unknown file type! {:?}", &f);
                }
            },
            _ => {
                panic!("unknown file type! {:?}", &f);
            }
        }
    }
    while in_process > 0 {
        if let Err(e) = py.check_signals() {
            eprintln!(
                "Decoder interrupted while waiting for worker threads. Remaining: {}/{}\n{}",
                in_process,
                files.len(),
                e
            );
            eprintln!("Completed: {:#?}", completed);
            eprintln!("Completed with errors: {:#?}", errored);
            break;
        };
        match receiver.try_recv() {
            Ok(r) => {
                update_done_files(&mut completed, &mut errored, r);
                in_process -= 1;
            }
            Err(_r) => {
                sleep(std::time::Duration::from_millis(100));
            }
        }
    }

    completed
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
    let line = LineString(coords).simplify_vw_idx(&precision);
    line.into_iter().collect::<Vec<usize>>()
}

/// This function is used internally by :func:`aisdb.denoising_encoder.encode_score`.
///
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
///     sqlite_dbpath (Option<String>)
///         If given, raw messages will be parsed and stored in an SQLite database at this location
///     postgres_connection_string (Option<String>)
///         Postgres database connection string
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
    sqlite_dbpath: Option<String>,
    postgres_connection_string: Option<String>,
    tcp_connect_addr: Option<String>,
    tcp_listen_addr: Option<String>,
    udp_listen_addr: Option<String>,
    multicast_addr_parsed: Option<String>,
    multicast_addr_raw: Option<String>,
    tcp_output_addr: Option<String>,
    udp_output_addr: Option<String>,
    dynamic_msg_bufsize: Option<usize>,
    static_msg_bufsize: Option<usize>,
    tee: Option<bool>,
    py: Python<'_>,
) {
    let threads = start_receiver(ReceiverArgs {
        sqlite_dbpath: sqlite_dbpath.map(PathBuf::from),
        postgres_connection_string,
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
    while threads
        .iter()
        .map(|t| t.is_finished())
        .filter(|b| !(*b))
        .count()
        > 0
    {
        py.check_signals().expect("Receiver interrupted");
        sleep(Duration::from_millis(500));
    }
}

/// Functions imported from Rust
#[pymodule]
#[allow(unused_variables)]
pub fn aisdb(py: Python, module: &PyModule) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(decoder))?;
    module.add_wrapped(wrap_pyfunction!(binarysearch_vector))?;
    module.add_wrapped(wrap_pyfunction!(encoder_score_fcn))?;
    module.add_wrapped(wrap_pyfunction!(haversine))?;
    module.add_wrapped(wrap_pyfunction!(receiver))?;
    module.add_wrapped(wrap_pyfunction!(simplify_linestring_idx))?;
    Ok(())
}
