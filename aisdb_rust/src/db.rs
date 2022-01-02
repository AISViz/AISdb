use futures::stream::iter;
use futures::StreamExt;
//use futures::stream::{self, StreamExt};
//use futures::StreamExt;

//use std::fs::read_dir;
use std::time::Instant;

//use futures::stream::iter;
use rusqlite::{Connection, Error, Result, Transaction};

#[path = "util.rs"]
mod util;

use crate::decodemsgs;
use crate::VesselData;
use util::glob_dir;

/// open a new database connection at the specified path
pub fn get_db_conn(path: Option<&str>) -> Result<Connection> {
    let conn = match path {
        Some(p) => Connection::open(p).unwrap(),
        None => Connection::open_in_memory().unwrap(),
    };
    println!("instantiating db connection: {:?}", path);
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = 0;
        PRAGMA cache_size = 10000000;
        PRAGMA temp_store = MEMORY;
        PRAGMA mmap_size = 30000000000;
        ",
    )
    .expect("PRAGMAS");

    Ok(conn)
}

/// TODO
//pub fn sqlite_createtable_staticreport(tx: &Transaction, mstr: &str) -> String {
//    format!("{}", mstr)
//}

/// create position reports table
pub fn sqlite_createtable_dynamicreport(tx: &Transaction, mstr: &str) -> Result<()> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS ais_{}_msg_dynamic(
            mmsi integer NOT NULL,
            time INTEGER,
            rot double precision,
            sog real,
            longitude double precision,
            latitude double precision,
            cog real,
            heading real,
            maneuver char,
            utc_second smallint,
            PRIMARY KEY (mmsi, time, longitude, latitude)
            --PRIMARY KEY (mmsi, time)
            )
        WITHOUT ROWID;",
        mstr
    );

    //let tx = tx.transaction().expect("creating tx");
    tx.execute(&sql, []).expect("executing in tx");
    //tx.commit().expect("committing tx");
    println!("/* creating table */\n{}", sql);
    Ok(())
}

/// TODO
/*
pub async fn sqlite_insert_static(
pool: &SqlitePool,
msgs: Vec<VesselData>,
mstr: &str,
) -> Result<(), Error> {
(pool, msgs, mstr);
Ok(())
}
*/

/// insert position reports into database
pub fn sqlite_insert_dynamic(tx: &Transaction, msgs: Vec<VesselData>, mstr: &str) -> Result<()> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_msg_dynamic
            (
              mmsi,
              time,
              longitude,
              latitude,
              rot,
              sog,
              cog,
              heading,
              maneuver,
              utc_second
            )
          VALUES (?,?,?,?,?,?,?,?,?,?)",
        mstr
    );
    let start = Instant::now();

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .expect("preparing statement");

    let mut n = 0;

    for msg in msgs {
        let (p, e) = msg.dynamicdata();
        let _ = stmt
            .execute([
                p.mmsi,
                e as u32,
                p.longitude.unwrap_or(0.) as u32,
                p.latitude.unwrap_or(0.) as u32,
                p.rot.unwrap_or(-1.) as u32,
                p.sog_knots.unwrap_or(-1.) as u32,
                p.cog.unwrap_or(-1.) as u32,
                p.heading_true.unwrap_or(-1.) as u32,
                p.special_manoeuvre.unwrap_or(false) as u32,
                p.timestamp_seconds.into(),
            ])
            .expect("executing prepared row");
        n += 1;
    }

    let elapsed = start.elapsed();
    println!(
        "inserted: {} msgs/s    elapsed: {}s    count: {}",
        n as f32 / elapsed.as_secs_f32(),
        elapsed.as_secs_f32(),
        n,
    );

    Ok(())
}

/// parse files and insert into DB using concurrent asynchronous runners
pub async fn concurrent_insert_dir(
    rawdata_dir: &str,
    dbpath: Option<&str>,
    start: Option<usize>,
) -> Result<(), Error> {
    assert_eq!(rawdata_dir.rsplit_once("/").unwrap().1, "");

    let mstr = "202111"; // TODO: parse from filepaths

    let mut conn = get_db_conn(dbpath).expect("getting db conn");
    let tx = conn.transaction().expect("begin transaction");
    let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating table!");
    tx.commit().expect("commit to DB!");

    let fpaths: Vec<String> = glob_dir("testdata/", "nm4", 0).expect("globbing");
    //let f_db_paths: Vec<(&String, Option<&str>)> = fpaths.iter().map(|f| (f, dbpath)).collect();
    let fpaths_conn: Vec<(&String, Connection)> = fpaths
        .iter()
        .map(|f| (f, get_db_conn(dbpath).expect("getting db conn")))
        .collect();

    let _stream = iter(fpaths_conn)
        .for_each_concurrent(4, |(f, mut c)| async move {
            let (positions, stat_msgs) = decodemsgs(&f);
            let t = c.transaction().expect("begin transaction");
            sqlite_insert_dynamic(&t, positions, mstr).expect("couldnt insert position reports");
            t.commit().expect("commit to DB!");
        })
        .await;

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decodemsgs;
    use util::glob_dir;

    //#[tokio::test]
    //#[async_std::test]
    #[test]
    fn test_create_dynamictable() -> Result<(), Error> {
        //let sqlite_pool = get_db_pool(None).await.expect("connecting to db");
        let mstr = "202111";
        let mut conn = get_db_conn(None).expect("getting db conn"); // memory db

        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        //sqlite_pool.close().await;
        //conn.close();

        Ok(())
    }

    #[test]
    fn test_insert_dynamic_msgs() -> Result<(), Error> {
        //let sqlite_pool = get_db_pool(Some("testdata/test.db"))
        //let sqlite_pool = get_db_pool(None).await.expect("connecting to db");
        //let conn = get_db_conn(None).unwrap(); // memory db
        let mstr = "202111";
        let dbpath = Some("testdata/test.db");

        let mut conn = get_db_conn(dbpath).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir("testdata/", "nm4", 0).unwrap();

        let mut conn = get_db_conn(dbpath).expect("getting db conn");
        for filepath in fpaths {
            if n > 5 {
                break;
            }
            n += 1;
            let (positions, _stat_msgs) = decodemsgs(&filepath);
            let tx = conn.transaction().expect("begin transaction");
            let _ = sqlite_insert_dynamic(&tx, positions, mstr);
            tx.commit().expect("commit to DB!");
            /*
               if &filepath[filepath.len() - 4..] == ".nm4" {
            //let _ = sqlite_insert_dynamic(
            //    get_db_conn(dbpath).expect("getting db conn"),
            //    positions,
            //    mstr,
            //);
            let _ = sqlite_insert_dynamic(tx, positions, mstr);
            tx.commit().expect("commit to DB!");
            } else {
            continue;
            };
            */
        }

        Ok(())
    }
    #[async_std::test]
    async fn test_concurrent_insert() {
        let dbpath = Some("testdata/test.db");
        let rawdata_dir = "testdata/";
        let _ = concurrent_insert_dir(rawdata_dir, dbpath, Some(0)).await;
    }
}
