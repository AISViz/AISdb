use std::fs::read_dir;

use sqlx::Error;
use tokio;

mod db;
mod decode;

use db::*;
use decode::*;

#[tokio::main]
/// first prototype only
/// decode dynamic reports into rust_test.db
pub async fn main() -> Result<(), Error> {
    //let sqlite_pool = get_db_pool(None).await.expect("connecting to db");
    let sqlite_pool = get_db_pool(Some("/run/media/matt/My Passport/testdb/rust_test.db"))
        .await
        .expect("connecting to db");

    sqlite_createtable_dynamicreport("202111", &sqlite_pool)
        .await
        .expect("inserting in db");

    let mut n = 0;

    for filepath in read_dir("testdata/")
        .unwrap()
        .map(|f| f.unwrap().path().display().to_string())
    {
        n += 1;
        if n < 21 {
            continue;
        } else if &filepath[filepath.len() - 4..] == ".nm4" {
            print!("{}\t", n);
            let (positions, stat_msgs) = decodemsgs(&filepath);
            sqlite_insert_positions(&sqlite_pool, positions, "202111")
                .await
                .expect("error calling async insert function");
        }
    }

    sqlite_pool.close().await;
    Ok(())
}
