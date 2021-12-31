use std::time::{Duration, Instant};

use sqlx::Error;
//use tokio;
//use async_std;

pub mod db;
pub mod decode;
pub mod util;

pub use db::*;
pub use decode::*;
pub use util::*;

pub use sqlx::query_as;

#[async_std::main]
/// first prototype
/// decode dynamic reports into rust_test.db
pub async fn main() -> Result<(), Error> {
    /*
     */
    let mstr = "202111";

    //let sqlite_pool = get_db_pool(None).await?;
    let sqlite_pool =
        get_db_pool(Some("/run/media/matt/My Passport/testdb/rust_vacuum.db")).await?;

    let _ = sqlite_createtable_dynamicreport(mstr, &sqlite_pool).await?;

    let start = Instant::now();
    //query_as("select count(*) from ais_202111_msg_dynamic;")
    let res: (i64,) = query_as(
        format!(
            //
            //"select count(*) from ais_{}_msg_dynamic"
            //"create index if not exists idx_{}_dynamic ON ais_{}_msg_dynamic(mmsi, time, longitude, latitude); "
            "create index if not exists idx_{}_mmsi ON ais_{}_msg_dynamic(mmsi);"
            //" create index if not exists idx_{}_time ON ais_{}_msg_dynamic(time)"
            //"VACUUM INTO '/run/media/matt/My Passport/testdb/rust_vacuum.db' "
            , mstr
            , mstr
        )
        .as_str(),
    )
    .fetch_one(&sqlite_pool)
    .await
    .expect("waiting for db...");
    let elapsed = start.elapsed();
    println!(
        "db count: {}   minutes: {}",
        res.0,
        elapsed.as_secs_f32() / 60.0
    );

    /*
    let _res = concurrent_insert_dir(
    "/run/media/matt/DATA_AIS/unzipped/",
    "/run/media/matt/My Passport/testdb/rust_test_newindex.db",
    Some(470),
    )
    .await
    .expect("could not spawn decoder processes");
    */

    Ok(())
}
