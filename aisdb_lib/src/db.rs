use crate::decode::VesselData;
use crate::util::epoch_2_dt;

extern crate include_dir;
use include_dir::{include_dir, Dir};

extern crate chrono;
use chrono::{DateTime, Utc};

extern crate rusqlite;
use rusqlite::{params, Connection, Result, Transaction};

static PROJECT_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../aisdb/aisdb_sql");

/// open a new database connection at the specified path
pub fn get_db_conn(path: &std::path::Path) -> Result<Connection> {
    let conn = match path.to_str().unwrap() {
        ":memory:" => Connection::open_in_memory().unwrap(),
        _ => Connection::open(path).unwrap(),
    };
    conn.busy_timeout(std::time::Duration::from_secs(300))?;

    let version_string = rusqlite::version();

    #[cfg(debug_assertions)]
    println!("SQLite3 version: {}", version_string);

    let vnum: Vec<i32> = version_string
        .split('.')
        .map(|s| s.parse().unwrap())
        .collect();

    if vnum[0] < 3 || vnum[0] == 3 && (vnum[1] < 8 || (vnum[1] == 8 && vnum[2] < 2)) {
        panic!("SQLite3 version is too low! Need version 3.8.2 or higher");
    }

    conn.execute_batch(
        "
        PRAGMA synchronous = 0;
        PRAGMA temp_store = MEMORY;
        ",
    )
    .unwrap_or_else(|_| panic!("setting PRAGMAS for {:?}", path.to_str()));

    Ok(conn)
}

/// embed SQL strings as literals
pub fn sql_from_file(fname: &str) -> &str {
    PROJECT_DIR
        .get_file(fname)
        .unwrap()
        .contents_utf8()
        .unwrap()
}

/// create position reports table
pub fn sqlite_createtable_dynamicreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sql = sql_from_file("createtable_dynamic_clustered.sql").replace("{}", mstr);
    Ok(tx
        .execute(&sql, [])
        .unwrap_or_else(|_| panic!("creating dynamic table\n{}", sql)))
}

/// create SQLite table for monthly static vessel reports
pub fn sqlite_createtable_staticreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sql = sql_from_file("createtable_static.sql").replace("{}", mstr);
    Ok(tx.execute(&sql, []).expect("creating static table"))
}

/// rtree index alternative to ais_month_dynamic clustered index.
/// faster read performance at the cost of up to 10x slower write and more disk space
/// currently not used
pub fn sqlite_create_rtree(tx: &Transaction, mstr: &str) -> Result<usize, rusqlite::Error> {
    // create rtree index as virtual table
    let sql1 = sql_from_file("createtable_dynamic_rtree.sql").replace("{}", mstr);
    tx.execute(&sql1, []).expect("creating rtree table");

    // populate rtree index automatically in the future
    let sql2 = sql_from_file("createtrigger_dynamic_rtreeidx.sql").replace("{}", mstr);
    tx.execute(&sql2, []).expect("creating rtree trigger");

    // populate rtree index manually from existing
    let sql3 = sql_from_file("insert_dynamic_rtreeidx.sql").replace("{}", mstr);
    Ok(tx.execute(&sql3, []).expect("inserting into rtree"))
}

/// insert static reports into database
pub fn sqlite_insert_static(
    tx: &Transaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> Result<()> {
    let sql = sql_from_file("insert_static.sql").replace("{}", mstr);

    let mut stmt = tx.prepare_cached(&sql)?;
    for msg in msgs {
        let (p, e) = msg.staticdata();

        let eta = p.eta.unwrap_or(DateTime::<Utc>::MIN_UTC);
        stmt.execute(params![
            p.mmsi,
            e,
            p.name.unwrap_or_default(),
            p.ship_type as i32,
            p.call_sign.unwrap_or_default(),
            p.imo_number.unwrap_or_default(),
            p.dimension_to_bow.unwrap_or_default(),
            p.dimension_to_stern.unwrap_or_default(),
            p.dimension_to_port.unwrap_or_default(),
            p.dimension_to_starboard.unwrap_or_default(),
            p.draught10.unwrap_or_default(),
            p.destination.unwrap_or_default(),
            p.ais_version_indicator,
            p.equipment_vendor_id.unwrap_or_default(),
            eta.format("%m").to_string(),
            eta.format("%d").to_string(),
            eta.format("%H").to_string(),
            eta.format("%M").to_string(),
            source,
        ])?;
    }
    Ok(())
}

/// insert position reports into database
pub fn sqlite_insert_dynamic(
    tx: &Transaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> Result<()> {
    let sql = sql_from_file("insert_dynamic_clusteredidx.sql").replace("{}", mstr);

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .unwrap_or_else(|_| panic!("preparing SQL statement:\n{}", sql));

    for msg in msgs {
        let (p, e) = msg.dynamicdata();
        let _ = stmt
            .execute(params![
                p.mmsi,
                e,
                p.longitude.unwrap_or_default(),
                p.latitude.unwrap_or_default(),
                p.rot.unwrap_or_default(),
                p.sog_knots.unwrap_or_default(),
                p.cog.unwrap_or_default(),
                p.heading_true.unwrap_or_default(),
                p.special_manoeuvre.unwrap_or_default(),
                p.timestamp_seconds,
                source,
            ])
            .unwrap_or_else(|_| {
                panic!("executing prepared row on {}", tx.path().unwrap().display())
            });
    }

    Ok(())
}

/// prepare a new transaction, ensure tables are created, and insert dynamic messages
pub fn prepare_tx_dynamic(
    c: &mut Connection,
    source: &str,
    positions: Vec<VesselData>,
) -> Result<()> {
    let mstr = epoch_2_dt(*positions[positions.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    sqlite_createtable_dynamicreport(&t, &mstr).expect("creating dynamic table");
    sqlite_insert_dynamic(&t, positions, &mstr, source).expect("insert dynamic");
    t.commit()
}

/// prepare a new transaction, ensure tables are created, and insert static messages
pub fn prepare_tx_static(
    c: &mut Connection,
    source: &str,
    stat_msgs: Vec<VesselData>,
) -> Result<()> {
    let mstr = epoch_2_dt(*stat_msgs[stat_msgs.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    sqlite_createtable_staticreport(&t, &mstr).expect("create static table");
    sqlite_insert_static(&t, stat_msgs, &mstr, source).unwrap_or_else(|_| panic!("insert static"));
    t.commit()
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

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
}
