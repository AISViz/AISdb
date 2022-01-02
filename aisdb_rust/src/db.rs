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

pub fn sqlite_createtable_staticreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    tx.execute(
        "
        CREATE TABLE IF NOT EXISTS ais_{}_msg_static (
            mmsi INTEGER,
            time INTEGER,
            --region INTEGER,
            --country INTEGER,
            --base_station integer,
            vessel_name TEXT,
            call_sign TEXT,
            imo INTEGER,
            --ship_type INTEGER,
            ship_type TEXT,
            dim_bow INTEGER,
            dim_stern INTEGER,
            dim_port INTEGER,
            dim_star INTEGER,
            draught INTEGER,
            destination TEXT,
            ais_version TEXT,
            fixing_device INTEGER,
            eta_month INTEGER,
            eta_day INTEGER,
            eta_hour INTEGER,
            eta_minute INTEGER,
            PRIMARY KEY (mmsi, time, imo)
        ) WITHOUT ROWID;
        ",
        [mstr],
    )
}

/// create position reports table
pub fn sqlite_createtable_dynamicreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS ais_{}_msg_dynamic (
            mmsi integer NOT NULL,
            time INTEGER,
            rot FLOAT,
            sog FLOAT,
            longitude FLOAT,
            latitude FLOAT,
            cog FLOAT,
            heading FLOAT,
            maneuver TEXT,
            utc_second INTEGER,
            PRIMARY KEY (mmsi, time, longitude, latitude)
            )
        WITHOUT ROWID;",
        mstr
    );

    //let tx = tx.transaction().expect("creating tx");
    tx.execute(&sql, [])
    //tx.commit().expect("committing tx");
    //Ok(())
}

pub fn sqlite_insert_static(tx: &Transaction, msgs: Vec<VesselData>, mstr: &str) -> Result<()> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_msg_static
            (
            mmsi,
            time,
            vessel_name,
            call_sign ,
            imo ,
            --ship_type ,
            dim_bow ,
            dim_stern ,
            dim_port ,
            dim_star ,
            draught ,
            destination ,
            ais_version ,
            fixing_device ,
            eta_month ,
            eta_day ,
            eta_hour ,
            eta_minute ,
            )
            ",
        mstr
    );
    println!("{}", sql);

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .expect("preparing statement");

    for msg in msgs {
        let (p, e) = msg.staticdata();
        let _ = stmt
            .execute([
                p.mmsi,
                e as u32,
                p.name.unwrap().parse::<u32>().unwrap_or(0),
                p.call_sign.unwrap().parse::<u32>().unwrap_or(0),
                p.imo_number.unwrap_or(0),
                //p.ship_type,
                p.dimension_to_bow.unwrap_or(0) as u32,
                p.dimension_to_stern.unwrap_or(0) as u32,
                p.dimension_to_port.unwrap_or(0) as u32,
                p.dimension_to_starboard.unwrap_or(0) as u32,
                p.draught10.unwrap_or(0) as u32,
                p.destination.unwrap().parse::<u32>().unwrap_or(0),
                p.ais_version_indicator as u32,
                p.equipment_vendor_id.unwrap().parse::<u32>().unwrap_or(0),
                p.eta
                    .unwrap()
                    .date()
                    .naive_utc()
                    .format("%m")
                    .to_string()
                    .parse::<u32>()
                    .unwrap_or(0),
                p.eta
                    .unwrap()
                    .date()
                    .naive_utc()
                    .format("%d")
                    .to_string()
                    .parse::<u32>()
                    .unwrap_or(0),
                p.eta
                    .unwrap()
                    .date()
                    .naive_utc()
                    .format("%H")
                    .to_string()
                    .parse::<u32>()
                    .unwrap_or(0),
                p.eta
                    .unwrap()
                    .date()
                    .naive_utc()
                    .format("%M")
                    .to_string()
                    .parse::<u32>()
                    .unwrap_or(0),
            ])
            .expect("executing prepared row");
    }
    //tx.commit().expect("commit");
    Ok(())

    //tx.execute(&sql, [mstr])
}

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
    start: usize,
    end: usize,
) -> Result<()> {
    let fpaths: Vec<String> = glob_dir(rawdata_dir, "nm4", 0).expect("globbing");
    let fpaths_rng = &fpaths.as_slice()[start..min(end, fpaths.len())];

    iter(fpaths_rng)
        .for_each_concurrent(2, |f| async move {
            let (positions, stat_msgs) = decodemsgs(&f);
            let filedate: DateTime<Utc> = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(*positions[0].epoch.as_ref().unwrap() as i64, 0),
                Utc,
            );
            let mstr = filedate.format("%Y%m").to_string();
            print!("\tmstr: {}", mstr);
            let mut c = get_db_conn(dbpath).expect("getting db conn");
            let t = c.transaction().expect("begin transaction");
            let _newtab1 = sqlite_createtable_staticreport(&t, &mstr).expect("creating table");
            let _newtab2 = sqlite_createtable_dynamicreport(&t, &mstr).expect("creating table");
            let _insert1 = sqlite_insert_static(&t, stat_msgs, &mstr).expect("insert static");
            let _insert2 = sqlite_insert_dynamic(&t, positions, &mstr).expect("insert positions");
            let _results = t.commit().expect("commit to db");
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
        let pargs = parse_args();
        let args = match pargs {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: {}", e);
                std::process::exit(1);
            }
        };

        let mstr = "00test00";
        let mut conn = get_db_conn(Some(&args.dbpath)).expect("getting db conn"); // memory db

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
    }
    #[test]
    fn test_insert_static_msgs() -> Result<()> {
        let mstr = "00test00";
        let pargs = parse_args();
        let args = match pargs {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: {}", e);
                std::process::exit(1);
            }
        };

        let mut conn = get_db_conn(Some(&args.dbpath)).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_staticreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir(&args.rawdata_dir, "nm4", 0).unwrap();

        let mut conn = get_db_conn(Some(&args.dbpath)).expect("getting db conn");
        for filepath in fpaths {
            if n > 5 {
                break;
            }
            n += 1;
            let (_, stat_msgs) = decodemsgs(&filepath);
            let tx = conn.transaction().expect("begin transaction");
            let _ = sqlite_insert_static(&tx, stat_msgs, mstr);
            tx.commit().expect("commit to DB!");
        }

        Ok(())
    }

    #[test]
    fn test_insert_dynamic_msgs() -> Result<()> {
        let mstr = "00test00";
        let pargs = parse_args();
        let args = match pargs {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: {}", e);
                std::process::exit(1);
            }
        };
        let mut conn = get_db_conn(Some(&args.dbpath)).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir(&args.rawdata_dir, "nm4", 0).unwrap();

        let mut conn = get_db_conn(Some(&args.dbpath)).expect("getting db conn");
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
        let args = parse_args().unwrap();
        //let dbpath = Some("testdata/test.db");
        //let rawdata_dir = "testdata/";
        //let (dbpath, rawdata_dir) = (&args[1], &args[2]);
        let _ = concurrent_insert_dir(&args.rawdata_dir, Some(&args.dbpath), 0, 5).await;
    }
}
