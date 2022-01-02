use futures::stream::iter;
use futures::StreamExt;
//use futures::stream::{self, StreamExt};
//use futures::StreamExt;

//use std::fs::read_dir;
use std::cmp::min;
use std::time::Instant;

//use futures::stream::iter;
use chrono::{DateTime, NaiveDateTime, Utc};
use rusqlite::{Connection, Result, Transaction};

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
pub fn sqlite_createtable_dynamicreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
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
    tx.execute(&sql, [])
    //tx.commit().expect("committing tx");
    //Ok(())
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
            //.expect("executing prepared row");
            ;
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
    start: usize,
    end: usize,
) -> Result<()> {
    assert_eq!(rawdata_dir.rsplit_once("/").unwrap().1, "");

    let fpaths: Vec<String> = glob_dir(rawdata_dir, "nm4", 0).expect("globbing");
    let fpaths_rng = &fpaths.as_slice()[start..min(end, fpaths.len())];

    iter(fpaths_rng)
        .for_each_concurrent(2, |f| async move {
            let (positions, stat_msgs) = decodemsgs(&f);
            let filedate: DateTime<Utc> = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(*positions[0].epoch.as_ref().unwrap() as i64, 0),
                Utc,
            );
            //let mstr = format!("{}{}", filedate.year, filedate.);
            let mstr = filedate.format("%Y%m").to_string();
            println!("mstr: {}", mstr);
            let mut c = get_db_conn(dbpath).expect("getting db conn");
            let t = c.transaction().expect("begin transaction");
            let _newtab = sqlite_createtable_dynamicreport(&t, &mstr).expect("creating table");
            let _insert = sqlite_insert_dynamic(&t, positions, &mstr).expect("insert positions");
            let _result = t.commit().expect("commit to db");
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
    use util::parse_args;

    #[test]
    fn test_create_dynamictable() -> Result<()> {
        let (dbpath, _rawdata_dir) = parse_args();

        let mstr = "202111";
        let mut conn = get_db_conn(Some(&dbpath)).expect("getting db conn"); // memory db

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
    }

    #[test]
    fn test_insert_dynamic_msgs() -> Result<()> {
        //let sqlite_pool = get_db_pool(Some("testdata/test.db"))
        //let sqlite_pool = get_db_pool(None).await.expect("connecting to db");
        //let conn = get_db_conn(None).unwrap(); // memory db
        let mstr = "202111";
        let (dbpath, rawdata_dir) = parse_args();
        //let dbpath = Some("testdata/test.db");

        let mut conn = get_db_conn(Some(&dbpath)).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir(&rawdata_dir, "nm4", 0).unwrap();

        let mut conn = get_db_conn(Some(&dbpath)).expect("getting db conn");
        for filepath in fpaths {
            if n > 5 {
                break;
            }
            n += 1;
            let (positions, _stat_msgs) = decodemsgs(&filepath);
            let tx = conn.transaction().expect("begin transaction");
            let _ = sqlite_insert_dynamic(&tx, positions, mstr);
            tx.commit().expect("commit to DB!");
        }

        Ok(())
    }
    #[async_std::test]
    async fn test_concurrent_insert() {
        let (dbpath, rawdata_dir) = parse_args();
        //let dbpath = Some("testdata/test.db");
        //let rawdata_dir = "testdata/";
        //let (dbpath, rawdata_dir) = (&args[1], &args[2]);
        let _ = concurrent_insert_dir(&rawdata_dir, Some(&dbpath), 0, 5).await;
    }
}
