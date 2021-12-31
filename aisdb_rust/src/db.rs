use futures::stream::iter;
use futures::StreamExt;
//use futures::*;
use std::fs::read_dir;
use std::str::FromStr;
use std::time::{Duration, Instant};

//use async_std::position;

use crate::util::glob_dir;
//use tokio::test;
//use tokio_stream::StreamExt;

use sqlx::{
    query, query_as,
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
        "CREATE TABLE IF NOT EXISTS ais_{}_msg_dynamic(
            --id INTEGER NOT NULL,
            mmsi integer NOT NULL,
            time INTEGER,
            --millisecond INTEGER,
            --msgtype INTEGER,
            --base_station integer,
            --navigational_status smallint,
            rot double precision,
            sog real,
            longitude double precision,
            latitude double precision,
            cog real,
            heading real,
            maneuver char,
            utc_second smallint
            --, PRIMARY KEY (mmsi, time, longitude, latitude)
            )
        --WITHOUT ROWID
        ;",
        mstr
    );

    println!("/* creating table */\n{}", sql);
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
    (pool, msgs, mstr);
    Ok(())
}

/// insert position reports into database
pub async fn sqlite_insert_positions(
    pool: &SqlitePool,
    msgs: Vec<VesselData>,
    mstr: &str,
) -> Result<(), Error> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_msg_dynamic
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

    let n = &msgs.len();
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

    // if sizeof table > 75000000
    //      vacuum into :
    //      Some(&dbpath)

    let res: (i64,) = query_as(format!("select count(*) from ais_{}_msg_dynamic", mstr).as_str())
        .fetch_one(&mut tx)
        .await?;

    let _ = tx.commit().await.expect("could not commit transaction");
    let elapsed = start.elapsed();

    println!(
        "inserted: {} msgs/s    elapsed: {}s    count: {}    total: {}",
        *n as f32 / elapsed.as_secs_f32(),
        elapsed.as_secs_f32(),
        *n,
        res.0
    );

    Ok(())
}

/// database pool for providing transaction context
pub async fn get_db_pool(path: Option<&str>) -> Result<SqlitePool, Error> {
    let connection_options = SqliteConnectOptions::from_str(&path.unwrap_or("sqlite://:memory:"))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Persist)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(600));

    let sqlite_pool = SqlitePoolOptions::new()
        .max_connections(2)
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

    let sqlite_pool = get_db_pool(Some(dbpath)).await.expect("connecting to db");
    sqlite_createtable_dynamicreport("202111", &sqlite_pool)
        .await
        .expect("creating dynamic tables");

    let skip = start.unwrap_or(0);
    let mut files: Vec<(String, &Pool<Sqlite>)> = read_dir(rawdata_dir)
        .unwrap()
        .map(|f| f.unwrap().path().display().to_string())
        .filter(|f| &f[f.len() - 4..] == ".nm4")
        .map(|f| (f, &sqlite_pool))
        .collect::<Vec<(String, &Pool<Sqlite>)>>()[skip..]
        .to_vec();

    files.sort_by(|a, b| a.0.cmp(&b.0));

    let _stream = iter(files)
        .for_each_concurrent(2, |(f, p)| async move {
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

    //#[tokio::test]
    #[async_std::test]
    async fn test_create_dynamictable() -> Result<(), Error> {
        let sqlite_pool = get_db_pool(None).await.expect("connecting to db");

        sqlite_createtable_dynamicreport("202111", &sqlite_pool)
            .await
            .expect("creating tables");

        sqlite_pool.close().await;

        Ok(())
    }

    #[async_std::test]
    async fn test_insert_dynamic_msgs() -> Result<(), Error> {
        //let sqlite_pool = get_db_pool(Some("testdata/test.db"))
        let sqlite_pool = get_db_pool(None).await.expect("connecting to db");
        let mstr = "202111";

        sqlite_createtable_dynamicreport("202111", &sqlite_pool)
            .await
            .expect("inserting in db");

        let mut n = 0;

        /*
        let mut fpaths = read_dir("testdata/")
            .unwrap()
            .map(|f| f.unwrap().path().display().to_string())
            .collect::<Vec<String>>();
        fpaths.sort();
        */
        let fpaths = glob_dir("testdata/", "nm4", 0).unwrap();

        for filepath in fpaths {
            if n > 10 {
                break;
            }
            n += 1;
            if &filepath[filepath.len() - 4..] == ".nm4" {
                let (positions, stat_msgs) = decodemsgs(&filepath);
                sqlite_insert_positions(&sqlite_pool, positions, mstr)
                    .await
                    .expect("could not insert!");
                sqlite_insert_static(&sqlite_pool, stat_msgs, mstr)
                    .await
                    .expect("could not insert!");
            } else {
                continue;
            };
        }

        sqlite_pool.close().await;
        Ok(())
    }
}
