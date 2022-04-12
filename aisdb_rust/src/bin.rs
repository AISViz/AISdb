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

//use futures::stream::iter;
//use futures::StreamExt;

use nmea_parser::NmeaParser;

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

    let mut parser = NmeaParser::new();
    for (d, f) in &path_arr {
        if f.to_str().unwrap().contains(&".nm4")
            || f.to_str().unwrap().contains(&".NM4")
            || f.to_str().unwrap().contains(&".RX")
            || f.to_str().unwrap().contains(&".rx")
        {
            parser = decode_insert_msgs(&d, &f, parser)
                .await
                .expect("decoding NM4");
        } else if f.to_str().unwrap().contains(&".csv") || f.to_str().unwrap().contains(&".CSV") {
            decodemsgs_ee_csv(&d, &f).await.expect("decoding CSV");
        } else {
            panic!("unknown file extension {:?}", &d);
        }
    }

    // create a future for the database call
    /*
    let mut insertfile = vec![];
    for (d, f) in path_arr {
        insertfile.push(async move {
            if f.to_str().unwrap().contains(&".nm4") || f.to_str().unwrap().contains(&".NM4") {
                parser = decode_insert_msgs(&d, &f, parser).await.expect("decoding");
            } else {
                decodemsgs_ee_csv(&d, &f).await.expect("decoding CSV");
            }
        });
    }
    let _results = futures::future::join_all(insertfile).await;
    */

    /* PARALLEL ASYNC */
    /* does not work with sqlite */
    // let handles = insertfile
    //    .into_iter()
    //    .map(async_std::task::spawn)
    //    .collect::<Vec<_>>();
    //let _results = futures::future::join_all(handles).await;

    /*
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
                //decode_insert_msgs(&d, &f.path()).await.expect("decoding")
                if f.path().to_str().unwrap().contains(&".nm4")
                    || f.path().to_str().unwrap().contains(&".NM4")
                {
                    decode_insert_msgs(&d, &f.path()).await.expect("decoding");
                } else {
                    decodemsgs_ee_csv(&d, &f.path())
                        .await
                        .expect("decoding CSV");
                }
            })
            .await;
    }
    */

    Ok(())
}
