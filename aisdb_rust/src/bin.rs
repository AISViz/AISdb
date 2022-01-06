#[path = "db.rs"]
pub mod db;

#[path = "decode.rs"]
pub mod decode;

#[path = "util.rs"]
pub mod util;

pub use db::*;
pub use decode::*;
pub use util::*;

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

    println!(
        "creating database {:?} from files in {:?}",
        args.dbpath, args.rawdata_dir,
    );
    let start = Instant::now();

    // array tuples containing (dbpath, filepath)
    let mut n = 0;
    let mut path_arr = vec![];
    for file in args.files {
        n += 1;
        if n <= args.start {
            continue;
        } else if n > args.end {
            break;
        } else {
            path_arr.push((std::path::PathBuf::from(&args.dbpath), file));
        }
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

    let elapsed = start.elapsed();
    println!(
        "total insert time: {} minutes\n",
        elapsed.as_secs_f32() / 60.,
    );

    //let sql = "VACUUM INTO '/run/media/matt/My Passport/test_vacuum_rust.db'";
    /*
    let sql = format!(
    "VACUUM INTO '{}.vacuum'",
    &args.dbpath.as_os_str().to_str().unwrap()
    );
    let conn = get_db_conn(&args.dbpath).unwrap();
    conn.execute(&sql, []).unwrap();
    */

    Ok(())
}
