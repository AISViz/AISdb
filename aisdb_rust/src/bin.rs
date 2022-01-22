//! benchmarking rust for SQLite DB inserts
//!
//! compiles to ./target/release/aisdb
//!
//! ``` text
//! AISDB
//!   convert AIS data in .nm4 format to an SQLite database containing
//!   vessel position reports and static data reports
//!   (message types 1, 2, 3, 5, 18, 24, 27)
//!
//! USAGE:
//!   aisdb --dbpath DBPATH ... [OPTIONS]
//!
//! ARGS:
//!   --dbpath        SQLite database path
//!
//! OPTIONS:
//!   -h, --help      Prints this message
//!   --file          Path to .nm4 file. Can be repeated multiple times
//!   --rawdata_dir   Path to .nm4 data directory
//!
//! ```

use futures::stream::iter;
use futures::StreamExt;

#[path = "db.rs"]
pub mod db;

#[path = "decode.rs"]
pub mod decode;

#[path = "util.rs"]
pub mod util;

use db::*;
use decode::*;
use util::*;

/// Accepts command line args for dbpath and rawdata_dir.
/// A new database will be created from decoded messages at
/// the specified path
#[async_std::main]
pub async fn main() -> Result<(), Error> {
    // collect command line arguments
    let args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    };

    println!("loading database file {:?}", args.dbpath);
    let start = Instant::now();

    // array tuples containing (dbpath, filepath)
    //let mut n = 0;
    let mut path_arr = vec![];
    for file in args.files {
        //n += 1;
        /*
        if n <= args.start {
        continue;
        } else if n > args.end {
        break;
        } else {
        path_arr.push((std::path::PathBuf::from(&args.dbpath), file));
        }
        */
        path_arr.push((std::path::PathBuf::from(&args.dbpath), file));
    }

    // create a future for the database call
    let mut insertfile = vec![];
    for (d, f) in path_arr {
        insertfile.push(async move {
            decode_insert_msgs(&d, &f).await.expect("decoding");
        });
    }
    let _results = futures::future::join_all(insertfile).await;

    /* PARALLEL ASYNC */
    /* does not work with sqlite */
    // let handles = insertfile
    //    .into_iter()
    //    .map(async_std::task::spawn)
    //    .collect::<Vec<_>>();
    //let _results = futures::future::join_all(handles).await;

    if args.rawdata_dir.is_some() {
        let mut fpaths: Vec<_> = std::fs::read_dir(&args.rawdata_dir.unwrap())
            .unwrap()
            .map(|p| (std::path::PathBuf::from(&args.dbpath), p.unwrap()))
            .collect();

        fpaths.sort_by_key(|t| t.1.path());
        // same thing but iterating over files in rawdata_dir
        // uses different futures aggregation method ??
        iter(fpaths)
            .for_each_concurrent(2, |(d, f)| async move {
                decode_insert_msgs(&d, &f.path()).await.expect("decoding")
            })
            .await;
    }

    let elapsed = start.elapsed();
    println!(
        "total insert time: {} minutes\nvacuuming...",
        elapsed.as_secs_f32() / 60.,
    );

    let conn = get_db_conn(&args.dbpath).expect("get db conn");
    let _v = conn.execute("VACUUM;", []).expect("vacuum");
    let _r = conn.close();

    Ok(())
}
