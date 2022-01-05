use futures::stream::iter;
use futures::StreamExt;

use std::cmp::min;
use std::time::Instant;

use chrono::{DateTime, NaiveDateTime, Utc, MIN_DATETIME};
use rusqlite::{params, Connection, Result, Transaction};

#[path = "util.rs"]
mod util;

use crate::decodemsgs;
use crate::VesselData;
use util::glob_dir;

/// open a new database connection at the specified path
pub fn get_db_conn(path: &std::path::Path) -> Result<Connection> {
    let conn = match path.to_str().unwrap() {
        //Some([":memory:"].iter().collect()) =
        ":memory:" => Connection::open_in_memory().unwrap(),
        _ => Connection::open(path).unwrap(),
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
    let sql = format!(
        "
        CREATE TABLE IF NOT EXISTS ais_{}_msg_static (
            mmsi INTEGER,
            time INTEGER,
            vessel_name TEXT,
            call_sign TEXT,
            imo INTEGER,
            dim_bow INTEGER,
            dim_stern INTEGER,
            dim_port INTEGER,
            dim_star INTEGER,
            draught INTEGER,
            destination TEXT,
            ais_version TEXT,
            fixing_device STRING,
            eta_month INTEGER,
            eta_day INTEGER,
            eta_hour INTEGER,
            eta_minute INTEGER,
            PRIMARY KEY (mmsi, time, imo)
        ) WITHOUT ROWID;
        ",
        mstr
    )
    .replace("\n", " ");
    tx.execute(&sql, [])
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
        ) WITHOUT ROWID;",
        mstr
    );

    Ok(tx.execute(&sql, []).expect("creating static tables"))
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
            eta_minute
            )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ",
        mstr
    );

    let mut stmt = tx.prepare_cached(&sql)?;
    for msg in msgs {
        let (p, e) = msg.staticdata();
        //let _ = stmt
        let eta = p.eta.unwrap_or(MIN_DATETIME);
        stmt.execute(params![
            p.mmsi,
            e,
            p.name.unwrap_or("".to_string()),
            p.call_sign.unwrap_or("".to_string()),
            p.imo_number.unwrap_or(0),
            //p.ship_type,
            p.dimension_to_bow.unwrap_or(0),
            p.dimension_to_stern.unwrap_or(0),
            p.dimension_to_port.unwrap_or(0),
            p.dimension_to_starboard.unwrap_or(0),
            p.draught10.unwrap_or(0),
            p.destination.unwrap_or("".to_string()),
            p.ais_version_indicator,
            p.equipment_vendor_id.unwrap_or("".to_string()),
            eta.format("%m").to_string(),
            eta.format("%d").to_string(),
            eta.format("%H").to_string(),
            eta.format("%M").to_string(),
        ])?;
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
            .execute(params![
                p.mmsi,
                e,
                p.longitude.unwrap_or(0.),
                p.latitude.unwrap_or(0.),
                p.rot.unwrap_or(-1.),
                p.sog_knots.unwrap_or(-1.),
                p.cog.unwrap_or(-1.),
                p.heading_true.unwrap_or(-1.),
                p.special_manoeuvre.unwrap_or(false),
                p.timestamp_seconds,
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
    dbpath: &std::path::Path,
    start: usize,
    end: usize,
) -> Result<()> {
    let fpaths: Vec<String> = glob_dir(rawdata_dir, "nm4", 0).expect("globbing");
    let fpaths_rng = &fpaths.as_slice()[start..min(end, fpaths.len())];

    iter(fpaths_rng)
        // TODO: clean this up
        .for_each_concurrent(2, |f| async move {
            let (positions, stat_msgs) = decodemsgs(&f);
            let filedate: DateTime<Utc> = DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(*positions[0].epoch.as_ref().unwrap() as i64, 0),
                Utc,
            );
            let mstr = filedate.format("%Y%m").to_string();
            let mut c = get_db_conn(dbpath).expect("getting db conn");
            let t = c.transaction();
            let _newtab2 = sqlite_createtable_dynamicreport(&t.as_ref().unwrap(), &mstr)
                .expect("creating dynamic table");
            let _insert2 = sqlite_insert_dynamic(&t.as_ref().unwrap(), positions, &mstr)
                .expect("insert positions");

            let _ = t.unwrap().commit();
            let t = c.transaction().expect("new tx");

            let _newtab1 =
                sqlite_createtable_staticreport(&t, &mstr).expect("creating static table");
            let _ = t.commit();

            let t = c.transaction();
            let _insert1 = sqlite_insert_static(&t.unwrap(), stat_msgs, &mstr).expect("insert");
            //let _results = t.commit().expect("commit to db");
        })
        .await;

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use super::*;
    use crate::decodemsgs;
    use util::glob_dir;

    fn testing_dbpaths() -> [std::path::PathBuf; 2] {
        [
            Path::new(":memory:").to_path_buf(),
            [std::env::current_dir().unwrap().to_str().unwrap(), "ais.db"]
                .iter()
                .collect::<PathBuf>(),
        ]
    }

    #[test]
    fn test_create_dynamictable() -> Result<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
    }

    /// TODO: update this test
    /*
       #[test]
       fn test_insert_static_msgs() -> Result<()> {
       let mstr = "00test00";
       let pargs = parse_args();
       let args = match pargs {
       Ok(v) => v,
       Err(ref e) => {
       eprintln!("need to input dbpath!: {}", e);
    //std::process::exit(1);
    util::AppArgs {
    dbpath: Path::new(":memory:").to_path_buf(),
    rawdata_dir: pargs.unwrap().rawdata_dir,
    start: 0,
    end: 3,
    }
    }
    };

    let mut conn = get_db_conn(None).expect("getting db conn");
    let tx = conn.transaction().expect("begin transaction");
    let _ = sqlite_createtable_staticreport(&tx, mstr).expect("creating tables");
    tx.commit().expect("commit to DB!");

    let mut n = 0;

    let fpaths = glob_dir(&args.rawdata_dir, "nm4", 0).unwrap();

    let mut conn = get_db_conn(None).expect("getting db conn");
    for filepath in fpaths {
    if n > 2 {
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
    */

    #[test]
    fn test_insert_dynamic_msgs() -> Result<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir("testdata/", "nm4", 0).unwrap();

        for filepath in fpaths {
            if n > 3 {
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
        for p in testing_dbpaths() {
            println!("\nTESTING DATABASE {:?}", &p);
            let _ = concurrent_insert_dir(
                std::env::current_dir()
                    .unwrap()
                    .to_path_buf()
                    .to_str()
                    .unwrap(),
                &p,
                0,
                5,
            )
            .await;
        }
    }
}
