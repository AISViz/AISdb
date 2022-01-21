use chrono::MIN_DATETIME;
use rusqlite::{params, Connection, Result, Transaction};

use crate::VesselData;

/// open a new database connection at the specified path
pub fn get_db_conn(path: &std::path::Path) -> Result<Connection> {
    let conn = match path.to_str().unwrap() {
        ":memory:" => Connection::open_in_memory().unwrap(),
        _ => Connection::open(path).unwrap(),
    };
    conn.execute_batch(
        "
        --PRAGMA cache_size = 10000000;
        --PRAGMA mmap_size = 30000000000;
        PRAGMA synchronous = 0;
        PRAGMA temp_store = MEMORY;
        ",
    )
    .expect("PRAGMAS");

    Ok(conn)
}

/// create SQLite table for monthly static vessel reports
pub fn sqlite_createtable_staticreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sql = format!(
        "
        CREATE TABLE IF NOT EXISTS ais_{}_static (
            mmsi INTEGER,
            time INTEGER,
            vessel_name TEXT,
            ship_type INT,
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
    .replace('\n', " ");
    tx.execute(&sql, [])
}

/// create position reports table
pub fn sqlite_createtable_dynamicreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS ais_{}_dynamic (
            mmsi integer NOT NULL,
            time INTEGER,
            longitude FLOAT,
            latitude FLOAT,
            rot FLOAT,
            sog FLOAT,
            cog FLOAT,
            heading FLOAT,
            maneuver TEXT,
            utc_second INTEGER,
            PRIMARY KEY (mmsi, time, longitude, latitude)
        ) WITHOUT ROWID;",
        mstr
    );

    Ok(tx.execute(&sql, []).expect("creating dynamic tables"))
}

/// rtree index alternative to ais_month_dynamic clustered index.
/// faster read performance at the cost of up to 10x slower write and more disk space
/// currently not used
pub fn sqlite_create_rtree(tx: &Transaction, mstr: &str) -> Result<usize, rusqlite::Error> {
    let vtab = format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS rtree_{}_dynamic USING rtree(
            id,
            mmsi0, mmsi1,
            t0, t1,
            x0, x1,
            y0, y1,
            --+region smallint,
            --+country smallint,
            --+msgtype integer,
            --+navigational_status smallint,
            +rot double precision,
            +sog real,
            +cog real,
            +heading real,
            +maneuver text,
            +utc_second smallint
        ); ",
        mstr
    );

    let idx = format!(
        "
            CREATE TRIGGER IF NOT EXISTS idx_rtree_{}_dynamic
            AFTER INSERT ON ais_{}_dynamic
            BEGIN
                INSERT INTO rtree_{}_dynamic(
                    --id,
                    mmsi0, mmsi1, t0, t1, x0, x1, y0, y1,
                    --navigational_status,
                    rot, sog, cog,
                    heading, utc_second
                )
                VALUES (
                    --new.ROWID,
                    new.mmsi, new.mmsi, new.time, new.time,
                    new.longitude, new.longitude, new.latitude, new.latitude,
                    --new.navigational_status,
                    new.rot, new.sog, new.cog,
                    new.heading, new.utc_second
                )
            ; END
        ",
        mstr, mstr, mstr
    );
    let genvtab = format!(
        "
        INSERT INTO rtree_{}_dynamic (
                mmsi0, mmsi1, t0, t1,
                x0, x1, y0, y1,
                rot, sog, cog,
                heading, utc_second
        )
        SELECT mmsi, mmsi, time, time,
                longitude, longitude, latitude, latitude,
                rot, sog, cog,
                heading, utc_second
        FROM ais_{}_dynamic
        ORDER BY 1, 3, 5, 7 ",
        mstr, mstr
    );
    tx.execute(&vtab, [])
        .expect("creating dynamic virtual tables");
    tx.execute(&idx, [])
        .expect("creating dynamic virtual index");
    Ok(tx.execute(&genvtab, []).expect("creating rtree"))
}

/// insert static reports into database
pub fn sqlite_insert_static(tx: &Transaction, msgs: Vec<VesselData>, mstr: &str) -> Result<()> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_static
            (
            mmsi,
            time,
            vessel_name,
            ship_type,
            call_sign,
            imo,
            dim_bow,
            dim_stern,
            dim_port,
            dim_star,
            draught,
            destination,
            ais_version,
            fixing_device,
            eta_month,
            eta_day,
            eta_hour,
            eta_minute
            )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ",
        mstr
    );

    let mut stmt = tx.prepare_cached(&sql)?;
    for msg in msgs {
        let (p, e) = msg.staticdata();

        let eta = p.eta.unwrap_or(MIN_DATETIME);
        stmt.execute(params![
            p.mmsi,
            e,
            p.name.unwrap_or_else(|| "".to_string()),
            p.ship_type as i32,
            p.call_sign.unwrap_or_else(|| "".to_string()),
            p.imo_number.unwrap_or(0),
            p.dimension_to_bow.unwrap_or(0),
            p.dimension_to_stern.unwrap_or(0),
            p.dimension_to_port.unwrap_or(0),
            p.dimension_to_starboard.unwrap_or(0),
            p.draught10.unwrap_or(0),
            p.destination.unwrap_or_else(|| "".to_string()),
            p.ais_version_indicator,
            p.equipment_vendor_id.unwrap_or_else(|| "".to_string()),
            eta.format("%m").to_string(),
            eta.format("%d").to_string(),
            eta.format("%H").to_string(),
            eta.format("%M").to_string(),
        ])?;
    }
    Ok(())
}

/// insert position reports into database
pub fn sqlite_insert_dynamic(tx: &Transaction, msgs: Vec<VesselData>, mstr: &str) -> Result<()> {
    let sql = format!(
        "INSERT OR IGNORE INTO ais_{}_dynamic
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

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .expect("preparing statement");

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
    }

    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Result;
    use crate::decodemsgs;
    use crate::get_db_conn;
    use crate::glob_dir;
    use crate::sqlite_createtable_dynamicreport;
    use crate::sqlite_createtable_staticreport;
    use crate::sqlite_insert_dynamic;
    use crate::sqlite_insert_static;

    #[test]
    fn test_create_statictable() -> Result<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_staticreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
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

    #[test]
    fn test_insert_static_msgs() -> Result<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_staticreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir(std::path::PathBuf::from("testdata/"), "nm4").unwrap();

        for filepath in fpaths {
            if n > 3 {
                break;
            }
            n += 1;
            let (_positions, stat_msgs) = decodemsgs(&filepath);
            let tx = conn.transaction().expect("begin transaction");
            let _ = sqlite_insert_static(&tx, stat_msgs, mstr);
            tx.commit().expect("commit to DB!");
        }

        Ok(())
    }

    #[test]
    fn test_insert_dynamic_msgs() -> Result<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        let mut n = 0;

        let fpaths = glob_dir(std::path::PathBuf::from("testdata/"), "nm4").unwrap();

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
}
