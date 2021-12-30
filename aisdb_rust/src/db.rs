use std::fs::read_dir;
use std::str::FromStr;
use std::time::{Duration, Instant};

//use futures::{stream, StreamExt};
use futures::*;

//use tokio::test;
//use tokio_stream::StreamExt;

use sqlx::{
    query,
    sqlite::{
        Sqlite, SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions,
        SqliteQueryResult, SqliteSynchronous,
    },
    Error, Pool,
};

use crate::decode::{decodemsgs, VesselData};

/// TODO
pub async fn sqlite_createtable_staticreport(mstr: &str) -> String {
    format!("{}", mstr)
}

/// create position reports table
pub async fn sqlite_createtable_dynamicreport(
    mstr: &str,
    pool: &SqlitePool,
) -> Result<SqliteQueryResult, Error> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS ais_{}_msg_1_2_3 (
            --id INTEGER NOT NULL,
            mmsi integer NOT NULL,
            time INTEGER,
            --millisecond INTEGER,
            msgtype INTEGER,
            --base_station integer,
            navigational_status smallint,
            rot double precision,
            sog real,
            longitude double precision,
            latitude double precision,
            cog real,
            heading real,
            maneuver char,
            utc_second smallint,
            PRIMARY KEY (mmsi, time)
            )
        WITHOUT ROWID;",
        mstr
    );

    let mut tx = pool.begin().await?;
    let res = query(&sql).execute(&mut tx).await?;
    tx.commit().await?;
    Ok(res)
}

/// TODO
pub async fn sqlite_insert_static(
    pool: &SqlitePool,
    msgs: Vec<VesselData>,
    mstr: &str,
) -> Result<(), Error> {
    Ok(())
}

/// insert position reports into database
pub async fn sqlite_insert_positions(
    pool: &SqlitePool,
    msgs: Vec<VesselData>,
    mstr: &str,
) -> Result<(), Error> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_msg_1_2_3
                      (mmsi, time,
                      --msgtype,
                      longitude, latitude,
                      --navigational_status,
                      rot, sog, cog, heading, maneuver, utc_second)
                      VALUES (?,?,?,?,?,?,?,?,?,?)",
        mstr
    );

    let start = Instant::now();
    let mut tx = pool.begin().await.expect("error starting transaction");

    for msg in msgs {
        let (p, e) = msg.dynamicdata();
        query(&sql)
            .bind(p.mmsi)
            .bind(e)
            //.bind(0)
            .bind(p.longitude)
            .bind(p.latitude)
            //.bind(p.nav_status)
            .bind(p.rot)
            .bind(p.rot)
            .bind(p.sog_knots)
            .bind(p.cog)
            .bind(p.heading_true)
            .bind(p.special_manoeuvre)
            .bind(p.timestamp_seconds)
            .execute(&mut tx)
            .await
            .expect("err inserting");
    }
    let _ = tx.commit().await.expect("could not commit transaction");
    let elapsed = start.elapsed();
    println!("insert time: {}", elapsed.as_secs_f64());

    Ok(())
}

/// database pool for providing transaction context
pub async fn get_db_pool(path: Option<&str>) -> Result<SqlitePool, Error> {
    let connection_options = SqliteConnectOptions::from_str(&path.unwrap_or("sqlite://:memory:"))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(600));

    let sqlite_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_timeout(Duration::from_secs(600))
        .connect_with(connection_options)
        .await?;

    query("pragma mmap_size = 30000000000;")
        .execute(&sqlite_pool)
        .await
        .expect("couldnt set pragma mmap_size");
    query("pragma page_size = 4096;")
        .execute(&sqlite_pool)
        .await
        .expect("couldnt set pragma page_size");
    query("pragma temp_store = MEMORY;")
        .execute(&sqlite_pool)
        .await
        .expect("couldnt set pragma temp_store");

    Ok(sqlite_pool)
}

/// parse files and insert into DB using concurrent asynchronous runners
pub async fn concurrent_insert_dir(
    rawdata_dir: &str,
    dbpath: &str,
    start: Option<usize>,
) -> Result<(), Error> {
    assert_eq!(rawdata_dir.rsplit_once("/").unwrap().1, "");

    let sqlite_pool = get_db_pool(Some(&dbpath)).await.expect("connecting to db");
    sqlite_createtable_dynamicreport("202111", &sqlite_pool)
        .await
        .expect("creating dynamic tables");

    let skip = start.unwrap_or(0);
    let files: Vec<(String, &Pool<Sqlite>)> = read_dir(rawdata_dir)
        .unwrap()
        .map(|f| f.unwrap().path().display().to_string())
        .filter(|f| &f[f.len() - 4..] == ".nm4")
        .map(|f| (f, &sqlite_pool))
        .collect::<Vec<(String, &Pool<Sqlite>)>>()[skip..]
        .to_vec();

    let stream = stream::iter(files)
        .for_each_concurrent(1, |(f, p)| async move {
            let (positions, stat_msgs) = decodemsgs(&f);
            sqlite_insert_positions(&p, positions, "202111")
                .await
                .expect("couldnt insert position reports");
            sqlite_insert_static(&p, stat_msgs, "202111")
                .await
                .expect("couldnt insert static reports")
        })
        .await;

    sqlite_pool.close().await;
    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_create_dynamictable() -> Result<(), Error> {
        let sqlite_pool = get_db_pool(None).await.expect("connecting to db");

        sqlite_createtable_dynamicreport("202111", &sqlite_pool)
            .await
            .expect("creating tables");

        sqlite_pool.close().await;

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_dynamic_msgs() -> Result<(), Error> {
        //let sqlite_pool = get_db_pool(Some("testdata/test.db"))
        let sqlite_pool = get_db_pool(None).await.expect("connecting to db");

        sqlite_createtable_dynamicreport("202111", &sqlite_pool)
            .await
            .expect("inserting in db");

        for filepath in read_dir("aisdb_rust/testdata/")
            .unwrap()
            .map(|f| f.unwrap().path().display().to_string())
        {
            if &filepath[filepath.len() - 4..] == ".nm4" {
                let (positions, stat_msgs) = decodemsgs(&filepath);
                sqlite_insert_positions(&sqlite_pool, positions, "202111")
                    .await
                    .expect("could not insert!");
            }
        }

        sqlite_pool.close().await;
        Ok(())
    }
}
