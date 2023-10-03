use crate::decode::VesselData;
use crate::util::epoch_2_dt;

use chrono::{DateTime, Utc};
use include_dir::{include_dir, Dir};

#[cfg(feature = "postgres")]
pub use postgres::{Client as PGClient, NoTls, Transaction as PGTransaction};

#[cfg(feature = "sqlite")]
pub use rusqlite::{
    params, Connection as SqliteConnection, OpenFlags, Result as SqliteResult,
    Transaction as SqliteTransaction,
};

static PROJECT_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../aisdb/aisdb_sql");

/// embed SQL strings as literals
pub fn sql_from_file(fname: &str) -> &str {
    PROJECT_DIR
        .get_file(fname)
        .unwrap()
        .contents_utf8()
        .unwrap()
}

#[cfg(feature = "sqlite")]
/// open a new database connection at the specified path
pub fn get_db_conn(path: std::path::PathBuf) -> SqliteResult<SqliteConnection> {
    let conn = match path.to_str().unwrap() {
        x if x.contains("file:") => SqliteConnection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_READ_WRITE,
        )?,
        ":memory:" => SqliteConnection::open_in_memory().unwrap(),
        _ => SqliteConnection::open(&path).unwrap(),
    };

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
    /*
    let res: String = conn
        .prepare("PRAGMA journal_mode")?
        .query([])?
        .next()?
        .unwrap()
        .get(0)?;
    if res != "wal" {
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .unwrap_or_else(|_| panic!("setting PRAGMAS for {:?}", path.to_str()));
    }
    */

    Ok(conn)
}

#[cfg(feature = "postgres")]
pub fn get_postgresdb_conn(connect_str: &str) -> Result<PGClient, Box<dyn std::error::Error>> {
    // TLS is handled by gateway router
    let client = PGClient::connect(connect_str, NoTls)?;
    #[cfg(debug_assertions)]
    println!("Connected to postgres server");
    Ok(client)
}

#[cfg(feature = "sqlite")]
/// create position reports table
pub fn sqlite_createtable_dynamicreport(
    tx: &SqliteTransaction,
    mstr: &str,
) -> SqliteResult<usize, rusqlite::Error> {
    let sql = sql_from_file("createtable_dynamic_clustered.sql").replace("{}", mstr);
    Ok(tx
        .execute(&sql, [])
        .unwrap_or_else(|e| panic!("creating dynamic table\n{}\n{}", sql, e))
        .try_into()
        .unwrap())
}

#[cfg(feature = "postgres")]
/// create position reports table
pub fn postgres_createtable_dynamicreport(
    tx: &mut PGTransaction,
    mstr: &str,
) -> Result<u64, postgres::Error> {
    let sql = sql_from_file("psql_createtable_dynamic_noindex.sql").replace("{}", mstr);
    tx.execute(&sql, &[])
}

#[cfg(feature = "sqlite")]
/// create SQLite table for monthly static vessel reports
pub fn sqlite_createtable_staticreport(
    tx: &SqliteTransaction,
    mstr: &str,
) -> SqliteResult<usize, rusqlite::Error> {
    let sql = sql_from_file("createtable_static.sql").replace("{}", mstr);
    Ok(tx.execute(&sql, []).expect("creating static table"))
}

#[cfg(feature = "postgres")]
/// create SQLite table for monthly static vessel reports
pub fn postgres_createtable_staticreport(
    tx: &mut PGTransaction,
    mstr: &str,
) -> Result<u64, postgres::Error> {
    let sql = sql_from_file("createtable_static.sql").replace("{}", mstr);
    tx.execute(&sql, &[])
}

#[cfg(feature = "sqlite")]
/// insert static reports into database
pub fn sqlite_insert_static(
    tx: &SqliteTransaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> SqliteResult<()> {
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
            eta.format("%m").to_string().parse::<i32>().unwrap(),
            eta.format("%d").to_string().parse::<i32>().unwrap(),
            eta.format("%H").to_string().parse::<i32>().unwrap(),
            eta.format("%M").to_string().parse::<i32>().unwrap(),
            source,
        ])?;
    }
    Ok(())
}

#[cfg(feature = "postgres")]
/// insert static reports into database
pub fn postgres_insert_static(
    tx: &mut PGTransaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> Result<(), postgres::Error> {
    let sql = sql_from_file("insert_static.sql").replace("{}", mstr);

    let stmt = tx.prepare(&sql)?;
    for msg in msgs {
        let (p, e) = msg.staticdata();

        let eta = p.eta.unwrap_or(DateTime::<Utc>::MIN_UTC);
        tx.execute(
            &stmt,
            &[
                &(p.mmsi as i32),
                &(e as i32),
                &p.name.unwrap_or_default(),
                &(p.ship_type as i32),
                &p.call_sign.unwrap_or_default(),
                &(p.imo_number.unwrap_or_default() as i32),
                &(p.dimension_to_bow.unwrap_or_default() as i32),
                &(p.dimension_to_stern.unwrap_or_default() as i32),
                &(p.dimension_to_port.unwrap_or_default() as i32),
                &(p.dimension_to_starboard.unwrap_or_default() as i32),
                &(p.draught10.unwrap_or_default() as i32),
                &p.destination.unwrap_or_default(),
                &(p.ais_version_indicator as i32),
                &p.equipment_vendor_id.unwrap_or_default(),
                &eta.format("%m")
                    .to_string()
                    .parse::<i32>()
                    .unwrap_or_default(),
                &eta.format("%d")
                    .to_string()
                    .parse::<i32>()
                    .unwrap_or_default(),
                &eta.format("%H")
                    .to_string()
                    .parse::<i32>()
                    .unwrap_or_default(),
                &eta.format("%M")
                    .to_string()
                    .parse::<i32>()
                    .unwrap_or_default(),
                &source,
            ],
        )?;
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
/// insert position reports into database
pub fn sqlite_insert_dynamic(
    tx: &SqliteTransaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> SqliteResult<()> {
    let sql = sql_from_file("insert_dynamic_clusteredidx.sql").replace("{}", mstr);

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .unwrap_or_else(|e| panic!("preparing SQL statement:\n{}\n{}", sql, e));

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
            .unwrap_or_else(|e| panic!("executing prepared row\n{}", e));
    }

    Ok(())
}

#[cfg(feature = "postgres")]
/// insert position reports into database
pub fn postgres_insert_dynamic(
    tx: &mut PGTransaction,
    msgs: Vec<VesselData>,
    mstr: &str,
    source: &str,
) -> Result<(), postgres::Error> {
    let sql = sql_from_file("insert_dynamic_clusteredidx.sql").replace("{}", mstr);

    let stmt = tx.prepare(&sql)?;

    for msg in msgs {
        let (p, e) = msg.dynamicdata();
        let _ = tx.execute(
            &stmt,
            &[
                &(p.mmsi as i32),
                &(e as i32),
                &(p.longitude.unwrap_or_default() as f32),
                &(p.latitude.unwrap_or_default() as f32),
                &(p.rot.unwrap_or_default() as f32),
                &(p.sog_knots.unwrap_or_default() as f32),
                &(p.cog.unwrap_or_default() as f32),
                &(p.heading_true.unwrap_or_default() as f32),
                &p.special_manoeuvre.unwrap_or_default(),
                &(p.timestamp_seconds as i32),
                &source,
            ],
        )?;
    }

    Ok(())
}

#[cfg(feature = "sqlite")]
/// prepare a new transaction, ensure tables are created, and insert dynamic messages
pub fn sqlite_prepare_tx_dynamic(
    c: &mut SqliteConnection,
    source: &str,
    positions: Vec<VesselData>,
) -> SqliteResult<()> {
    let mstr = epoch_2_dt(*positions[positions.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    sqlite_createtable_dynamicreport(&t, &mstr).expect("creating dynamic table");
    sqlite_insert_dynamic(&t, positions, &mstr, source).expect("insert dynamic");
    t.commit()
}

#[cfg(feature = "postgres")]
/// prepare a new transaction, ensure tables are created, and insert dynamic messages
pub fn postgres_prepare_tx_dynamic(
    c: &mut PGClient,
    source: &str,
    positions: Vec<VesselData>,
) -> Result<(), postgres::Error> {
    let mstr = epoch_2_dt(*positions[positions.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let mut t = c.transaction()?;
    //postgres_createtable_dynamicreport(&mut t, &mstr)?;
    postgres_insert_dynamic(&mut t, positions, &mstr, source)?;
    t.commit()
}

#[cfg(feature = "sqlite")]
/// prepare a new transaction, ensure tables are created, and insert static messages
pub fn sqlite_prepare_tx_static(
    c: &mut SqliteConnection,
    source: &str,
    stat_msgs: Vec<VesselData>,
) -> SqliteResult<()> {
    let mstr = epoch_2_dt(*stat_msgs[stat_msgs.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    sqlite_createtable_staticreport(&t, &mstr).expect("create static table");
    sqlite_insert_static(&t, stat_msgs, &mstr, source)
        .unwrap_or_else(|e| panic!("insert static: {}", e));
    t.commit()
}

#[cfg(feature = "postgres")]
/// prepare a new transaction, ensure tables are created, and insert static messages
pub fn postgres_prepare_tx_static(
    c: &mut PGClient,
    source: &str,
    stat_msgs: Vec<VesselData>,
) -> Result<(), postgres::Error> {
    let mstr = epoch_2_dt(
        *stat_msgs[stat_msgs.len() - 1]
            .epoch
            .as_ref()
            .expect("get epoch") as i64,
    )
    .format("%Y%m")
    .to_string();
    let mut t = c.transaction()?;
    //postgres_createtable_staticreport(&mut t, &mstr)?;
    postgres_insert_static(&mut t, stat_msgs, &mstr, source)?;
    t.commit()
}

/*
#[cfg(feature = "sqlite")]
/// rtree index alternative to ais_month_dynamic clustered index.
/// faster read performance at the cost of up to 10x slower write and more disk space
/// currently not used
pub fn sqlite_create_rtree(
tx: &SqliteTransaction,
//tx: &dyn DatabaseTransaction<Transaction>,
mstr: &str,
) -> SqliteResult<usize, rusqlite::Error> {
//) -> Result<usize, Box<dyn std::error::Error>> {
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
*/

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_create_statictable() -> SqliteResult<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_staticreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
    }

    #[test]
    fn test_create_dynamictable() -> SqliteResult<()> {
        let mstr = "00test00";
        let mut conn = get_db_conn(Path::new(":memory:")).expect("getting db conn");

        println!("/* creating table */");
        let tx = conn.transaction().expect("begin transaction");
        let _ = sqlite_createtable_dynamicreport(&tx, mstr).expect("creating tables");
        tx.commit().expect("commit to DB!");

        Ok(())
    }
}
