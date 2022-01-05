use async_std;

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
    //let args: Vec<String> = env::args().collect();
    //let (dbpath, rawdata_dir) = (&args[1], &args[2]);

    let args = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    };

    println!("{:#?}", args);

    println!(
        "creating database {:?} from files in {:?}",
        args.dbpath, args.rawdata_dir,
    );

    let start = Instant::now();
    let _ = concurrent_insert_dir(&args.rawdata_dir, &args.dbpath, args.start, args.end).await;
    let elapsed = start.elapsed();

    println!(
        "total insert time: {} minutes\nvacuuming...",
        elapsed.as_secs_f32() / 60.,
    );

    //let sql = "VACUUM INTO '/run/media/matt/My Passport/test_vacuum_rust.db'";
    let sql = format!(
        "VACUUM INTO '{}.vacuum'",
        &args.dbpath.as_os_str().to_str().unwrap()
    );
    let conn = get_db_conn(&args.dbpath).unwrap();
    conn.execute(&sql, []).unwrap();

    Ok(())
}
