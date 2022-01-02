use std::time::Instant;

use async_std;
use std::env;

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
    let args: Vec<String> = env::args().collect();
    let (dbpath, rawdata_dir) = (&args[1], &args[2]);

    println!("creating database {} from files in {}", dbpath, rawdata_dir);

    let start = Instant::now();
    let _ = concurrent_insert_dir(rawdata_dir, Some(dbpath), Some(0)).await;
    let elapsed = start.elapsed();

    println!("total insert time: {} minutes", elapsed.as_secs_f32() / 60.);

    Ok(())
}
