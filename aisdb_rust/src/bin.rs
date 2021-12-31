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

#[tokio::main]
/// first prototype
/// decode dynamic reports into rust_test.db
pub async fn main() -> Result<(), Error> {
    /*
     */
    let mstr = "202111";

    let sqlite_pool = get_db_pool(None).await?;

    let _ = sqlite_createtable_dynamicreport(mstr, &sqlite_pool).await?;

    //query_as("select count(*) from ais_202111_msg_dynamic;")
    let res: (i64,) = query_as(format!("select count(*) from ais_{}_msg_dynamic", mstr).as_str())
        .fetch_one(&sqlite_pool)
        .await
        .expect("waiting for db...");
    println!("db count: {}", res.0);

    let _res = concurrent_insert_dir(
        "/run/media/matt/DATA_AIS/unzipped/",
        "/run/media/matt/My Passport/testdb/rust_test_newindex.db",
        //Some(15),
        Some(450),
    )
    .await
    .expect("could not spawn decoder processes");

    Ok(())
}
