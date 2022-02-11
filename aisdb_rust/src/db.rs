use std::env::current_exe;
use std::fs::read_to_string;

use chrono::MIN_DATETIME;
use rusqlite::{params, Connection, Result, Transaction};

use crate::util::epoch_2_dt;
use crate::VesselData;

/// open a new database connection at the specified path
pub fn get_db_conn(path: &std::path::Path) -> Result<Connection> {
    let conn = match path.to_str().unwrap() {
        ":memory:" => Connection::open_in_memory().unwrap(),
        _ => Connection::open(path).unwrap(),
    };
    conn.execute_batch(
        "
        PRAGMA synchronous = 0;
        PRAGMA temp_store = MEMORY;
        ",
    )
    .expect("PRAGMAS");

    Ok(conn)
}

/// get absolute path to SQL source code
pub fn sqlfiles_abspath(fname: &str) -> std::path::PathBuf {
    let mut exepath = current_exe().unwrap();
    while &exepath.to_str().unwrap()[&exepath.to_str().unwrap().len() - 10..] != "aisdb_rust" {
        exepath = exepath.parent().unwrap().to_path_buf();
    }
    exepath = exepath.parent().unwrap().to_path_buf();

    let mut exename = exepath.to_str().unwrap().to_string();
    exename += "/";
    exename += &format!("aisdb_sql/{}", fname).to_string();

    std::path::PathBuf::from(exename)
}

/// create position reports table
pub fn sqlite_createtable_dynamicreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sqlfile = read_to_string(sqlfiles_abspath("createtable_dynamic_clustered.sql")).expect(
        format!(
            "Error reading SQL from file: {:?}",
            sqlfiles_abspath("createtable_dynamic_clustered.sql")
        )
        .as_str(),
    );
    let sql = sqlfile.replace("{}", mstr);

    Ok(tx.execute(&sql, []).expect("creating dynamic table"))
}

/// create SQLite table for monthly static vessel reports
pub fn sqlite_createtable_staticreport(
    tx: &Transaction,
    mstr: &str,
) -> Result<usize, rusqlite::Error> {
    let sqlfile =
        read_to_string(sqlfiles_abspath("createtable_static.sql")).expect("reading SQL from file");
    let sql = sqlfile.replace("{}", mstr);
    Ok(tx.execute(&sql, []).expect("creating static table"))
}

/// rtree index alternative to ais_month_dynamic clustered index.
/// faster read performance at the cost of up to 10x slower write and more disk space
/// currently not used
pub fn sqlite_create_rtree(tx: &Transaction, mstr: &str) -> Result<usize, rusqlite::Error> {
    // create rtree index as virtual table
    let sqlfile1 = read_to_string(sqlfiles_abspath("createtable_dynamic_rtree.sql"))
        .expect("reading SQL from file");
    let sql1 = sqlfile1.replace("{}", mstr);
    tx.execute(&sql1, []).expect("creating rtree table");

    // populate rtree index automatically in the future
    let sqlfile2 = read_to_string(sqlfiles_abspath("createtrigger_dynamic_rtreeidx.sql"))
        .expect("reading SQL from file");
    let sql2 = sqlfile2.replace("{}", mstr);
    tx.execute(&sql2, []).expect("creating rtree trigger");

    // populate rtree index manually from existing
    let sqlfile3 = read_to_string(sqlfiles_abspath("insert_dynamic_rtreeidx.sql"))
        .expect("reading SQL from file");
    let sql3 = sqlfile3.replace("{}", mstr);
    Ok(tx.execute(&sql3, []).expect("inserting into rtree"))
}

/// insert static reports into database
pub fn sqlite_insert_static(tx: &Transaction, msgs: Vec<VesselData>, mstr: &str) -> Result<()> {
    let sqlfile =
        read_to_string(sqlfiles_abspath("insert_static.sql")).expect("reading SQL from file");
    let sql = sqlfile.replace("{}", mstr);

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
    let sqlfile = read_to_string(sqlfiles_abspath("insert_dynamic_clusteredidx.sql"))
        .expect("reading SQL from file");
    let sql = sqlfile.replace("{}", mstr);

    let mut stmt = tx
        .prepare_cached(sql.as_str())
        .expect(format!("preparing SQL statement:\n{}", sql).as_str());

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

/// prepare a new transaction, ensure tables are created, and insert dynamic messages
pub fn prepare_tx_dynamic(c: &mut Connection, positions: Vec<VesselData>) -> Result<()> {
    let mstr = epoch_2_dt(*positions[positions.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    let _c = sqlite_createtable_dynamicreport(&t, &mstr).expect("creating dynamic table");
    let _d = sqlite_insert_dynamic(&t, positions, &mstr).expect("insert dynamic");
    let _ = t.commit();
    Ok(())
}

/// prepare a new transaction, ensure tables are created, and insert static messages
pub fn prepare_tx_static(c: &mut Connection, stat_msgs: Vec<VesselData>) -> Result<()> {
    let mstr = epoch_2_dt(*stat_msgs[stat_msgs.len() - 1].epoch.as_ref().unwrap() as i64)
        .format("%Y%m")
        .to_string();
    let t = c.transaction().unwrap();
    let _c = sqlite_createtable_staticreport(&t, &mstr).expect("create static table");
    let _s = sqlite_insert_static(&t, stat_msgs, &mstr).expect("insert static");
    let _ = t.commit();
    Ok(())
}

/* --------------------------------------------------------------------------------------------- */

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::decode::decodemsgs;
    use crate::util::glob_dir;

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
