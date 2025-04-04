pub use std::{
    fs::{create_dir_all, read_dir, File},
    io::{BufRead, BufReader, Error, Write},
    time::{Duration, Instant},
    path::{Path},
    collections::{HashSet},
};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use csv::StringRecord;
use nmea_parser::ais::{
    AisClass, CargoType, NavigationStatus, ShipType, Station, VesselDynamicData, VesselStaticData,
};
use nmea_parser::ParsedMessage;

#[cfg(feature = "sqlite")]
use crate::db::{get_db_conn, sqlite_prepare_tx_dynamic, sqlite_prepare_tx_static};
#[cfg(feature = "postgres")]
use crate::db::{get_postgresdb_conn, postgres_prepare_tx_dynamic, postgres_prepare_tx_static};
use crate::decode::VesselData;

const BATCHSIZE: usize = 50000;

/// Convert time string to epoch seconds
pub fn csvdt_2_epoch(dt: &str) -> Result<i64, String> {
    let utctime = NaiveDateTime::parse_from_str(dt, "%Y%m%d_%H%M%S")
        .or_else(|_| NaiveDateTime::parse_from_str(dt, "%Y%m%dT%H%M%SZ"));

    match utctime {
        Ok(parsed_time) => Ok(Utc.from_utc_datetime(&parsed_time).timestamp()),
        Err(e) => Err(format!("Failed to parse timestamp '{}': {}", dt, e)),
    }
}

/// filter everything but vessel data, sort vessel data into static and dynamic vectors
pub fn filter_vesseldata_csv(rowopt: Option<StringRecord>) -> Option<(StringRecord, i32, bool)> {
    
    rowopt.as_ref()?;

    let row = rowopt.unwrap();
    let clonedrow = row.clone();
    let msgtype = clonedrow.get(1).unwrap();
    match msgtype {
        "1" | "2" | "3" | "18" | "19" | "27" => Some((
            row,
            csvdt_2_epoch(clonedrow.get(3).as_ref().unwrap()).unwrap_or_else(|e| {
                eprintln!("Failed to parse timestamp: {}", e);
                0 }) as i32,
            true,
        )),
        "5" | "24" => Some((
            row,
            csvdt_2_epoch(clonedrow.get(3).as_ref().unwrap()).unwrap_or_else(|e| {
                eprintln!("Failed to parse timestamp: {}", e);
                0 }) as i32,
            false,
        )),
        _ => None,
    }
}

/// convert ISO8601 time format (NOAA in use) to epoch seconds
pub fn iso8601_2_epoch(dt: &str) -> Option<i64> {
    match NaiveDateTime::parse_from_str(dt, "%Y-%m-%dT%H:%M:%S") {
        Ok(utctime) => Some(Utc.from_utc_datetime(&utctime).timestamp()),
        Err(_) => None,  // Return None instead of panicking
    }
}

/// Encodes the ETA into a 20-bit integer format
fn parse_eta(row: &csv::StringRecord) -> Option<DateTime<Utc>> {
    let eta_month: Option<u32> = row.get(45).and_then(|s| s.parse::<f64>().ok()).map(|x| x as u32);
    let eta_day: Option<u32> = row.get(46).and_then(|s| s.parse::<f64>().ok()).map(|x| x as u32);
    let eta_hour: Option<u32> = row.get(47).and_then(|s| s.parse::<f64>().ok()).map(|x| x as u32);
    let eta_minute: Option<u32> = row.get(48).and_then(|s| s.parse::<f64>().ok()).map(|x| x as u32);
    
    match (eta_month, eta_day, eta_hour, eta_minute) {
        (Some(month), Some(day), Some(hour), Some(minute))
            if (1..=12).contains(&month) &&
               (1..=31).contains(&day) &&
               (0..=23).contains(&hour) &&
               (0..=59).contains(&minute) =>
        {            
            // Use a fixed pseudo year - 2000, year in ETA will be discarded during insertion
            let pseudo_year = 2000;
            
            // Create a DateTime<Utc> with the pseudo year
            Utc.with_ymd_and_hms(pseudo_year, month, day, hour, minute, 0).single()
        }
        _ => None,
    }
}

/// perform database input from Spire
#[cfg(feature = "sqlite")]
pub fn sqlite_decodemsgs_ee_csv(
    dbpath: std::path::PathBuf,
    filename: std::path::PathBuf,
    source: &str,
    verbose: bool,
) -> Result<(), Error> {
    assert_eq!(&filename.extension().expect("getting file ext"), &"csv");

    let start = Instant::now();

    let mut reader = csv::Reader::from_reader(
        File::open(&filename).unwrap_or_else(|_| panic!("cannot open file {:?}", filename)),
    );
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();
    let mut count = 0;

    let mut c = get_db_conn(dbpath).expect("getting db conn");

    for (row, epoch, is_dynamic) in reader
        .records()
        .filter_map(|r| filter_vesseldata_csv(r.ok()))
    {
        count += 1;
        if is_dynamic {
            let payload = VesselDynamicData {
                own_vessel: true,
                station: Station::BaseStation,
                ais_type: AisClass::Unknown,
                mmsi: row.get(0).unwrap().parse().unwrap(),
                nav_status: NavigationStatus::NotDefined,
                rot: row.get(25).unwrap().parse::<f64>().ok(),
                rot_direction: None,
                sog_knots: row.get(26).unwrap().parse::<f64>().ok(),
                high_position_accuracy: false,
                latitude: row.get(29).unwrap().parse().ok(),
                longitude: row.get(28).unwrap().parse().ok(),
                cog: row.get(30).unwrap().parse().ok(),
                heading_true: row.get(31).unwrap().parse().ok(),
                timestamp_seconds: row.get(42).unwrap().parse::<u8>().unwrap_or(0),
                positioning_system_meta: None,
                current_gnss_position: None,
                special_manoeuvre: None,
                raim_flag: false,
                class_b_unit_flag: None,
                class_b_display: None,
                class_b_dsc: None,
                class_b_band_flag: None,
                class_b_msg22_flag: None,
                class_b_mode_flag: None,
                class_b_css_flag: None,
                radio_status: None,
            };
            let message = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselDynamicData(payload)),
            };
            positions.push(message);
        } else {
            let payload = VesselStaticData {
                own_vessel: true,
                ais_type: AisClass::Unknown,
                mmsi: row.get(0).unwrap().parse().unwrap(),
                ais_version_indicator: row.get(23).unwrap().parse().unwrap_or_default(),
                imo_number: row.get(15).unwrap().parse().ok(),
                call_sign: row.get(14).unwrap().parse().ok(),
                name: Some(row.get(13).unwrap_or("").to_string()),
                ship_type: ShipType::new(row.get(16).unwrap().parse().unwrap_or_default()),
                cargo_type: CargoType::Undefined,
                equipment_vendor_id: None,
                equipment_model: None,
                equipment_serial_number: None,
                dimension_to_bow: row.get(17).unwrap_or_default().parse().ok(),
                dimension_to_stern: row.get(18).unwrap_or_default().parse().ok(),
                dimension_to_port: row.get(19).unwrap_or_default().parse().ok(),
                dimension_to_starboard: row.get(20).unwrap_or_default().parse().ok(),
                position_fix_type: None,
                eta: parse_eta(&row),
                draught10: row.get(21).unwrap_or_default().parse().ok(),
                destination: row.get(22).unwrap_or_default().parse().ok(),
                mothership_mmsi: row.get(131).unwrap_or_default().parse().ok(),
            };
            let message = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselStaticData(payload)),
            };
            stat_msgs.push(message);
        }

        if positions.len() >= BATCHSIZE {
            let _d = sqlite_prepare_tx_dynamic(&mut c, source, positions);
            positions = vec![];
        };
        if stat_msgs.len() >= BATCHSIZE {
            let _s = sqlite_prepare_tx_static(&mut c, source, stat_msgs);
            stat_msgs = vec![];
        }
    }

    if !positions.is_empty() {
        let _d = sqlite_prepare_tx_dynamic(&mut c, source, positions);
    }
    if !stat_msgs.is_empty() {
        let _s = sqlite_prepare_tx_static(&mut c, source, stat_msgs);
    }

    let elapsed = start.elapsed();
    let f3 = filename.to_str().unwrap();
    let f4 = Path::new(f3);
    let fname = f4.file_name().unwrap().to_str().unwrap();
    let fname1 = format!("{:<1$}", fname, 64);
    let elapsed1 = format!(
        "elapsed: {:>1$}s",
        format!("{:.2 }", elapsed.as_secs_f32()),
        7
    );
    let rate1 = format!(
        "rate: {:>1$} msgs/s",
        format!("{:.0}", count as f32 / elapsed.as_secs_f32()),
        8
    );

    if verbose {
        println!(
            "{} count:{: >8}    {}    {}",
            fname1, count, elapsed1, rate1,
        );
    }

    Ok(())
}

/// perform database input from Spire data
#[cfg(feature = "postgres")]
pub fn postgres_decodemsgs_ee_csv(
    connect_str: &str,
    filename: &std::path::PathBuf,
    source: &str,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(&filename.extension().expect("getting file ext"), &"csv");

    let start = Instant::now();

    let mut reader = csv::Reader::from_reader(File::open(filename)?);
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();
    let mut count = 0;

    let mut c = get_postgresdb_conn(connect_str)?;

    for (row, epoch, is_dynamic) in reader
        .records()
        .filter_map(|r| filter_vesseldata_csv(r.ok()))
    {
        count += 1;
        if is_dynamic {
            let payload = VesselDynamicData {
                own_vessel: true,
                station: Station::BaseStation,
                ais_type: AisClass::Unknown,
                mmsi: row.get(0).unwrap().parse::<u32>().unwrap_or(0),  // make tolerant with invalid MMSIs
                nav_status: NavigationStatus::NotDefined,
                rot: row.get(25).unwrap().parse::<f64>().ok(),
                rot_direction: None,
                sog_knots: row.get(26).unwrap().parse::<f64>().ok(),
                high_position_accuracy: false,
                latitude: row.get(29).unwrap().parse().ok(),
                longitude: row.get(28).unwrap().parse().ok(),
                cog: row.get(30).unwrap().parse().ok(),
                heading_true: row.get(31).unwrap().parse().ok(),
                timestamp_seconds: row.get(42).unwrap().parse::<u8>().unwrap_or(0),
                positioning_system_meta: None,
                current_gnss_position: None,
                special_manoeuvre: None,
                raim_flag: false,
                class_b_unit_flag: None,
                class_b_display: None,
                class_b_dsc: None,
                class_b_band_flag: None,
                class_b_msg22_flag: None,
                class_b_mode_flag: None,
                class_b_css_flag: None,
                radio_status: None,
            };
            let message = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselDynamicData(payload)),
            };
            positions.push(message);
        } else {
            let payload = VesselStaticData {
                own_vessel: true,
                ais_type: AisClass::Unknown,
                mmsi: row.get(0).unwrap().parse().unwrap(),
                ais_version_indicator: row.get(23).unwrap().parse().unwrap_or_default(),
                imo_number: row.get(15).unwrap().parse().ok(),
                call_sign: row.get(14).unwrap().parse().ok(),
                name: Some(row.get(13).unwrap_or("").to_string()),
                ship_type: ShipType::new(row.get(16).unwrap().parse().unwrap_or_default()),
                cargo_type: CargoType::Undefined,
                equipment_vendor_id: None,
                equipment_model: None,
                equipment_serial_number: None,
                dimension_to_bow: row.get(17).unwrap_or_default().parse().ok(),
                dimension_to_stern: row.get(18).unwrap_or_default().parse().ok(),
                dimension_to_port: row.get(19).unwrap_or_default().parse().ok(),
                dimension_to_starboard: row.get(20).unwrap_or_default().parse().ok(),
                position_fix_type: None,
                // eta: None,  // at cols 45-48 ETA month, day, hour, minute, format: float64
                eta: parse_eta(&row),
                draught10: row.get(21).unwrap_or_default().parse().ok(),
                destination: row.get(22).unwrap_or_default().parse().ok(),
                mothership_mmsi: row.get(131).unwrap_or_default().parse().ok(),
            };
            let message = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselStaticData(payload)),
            };
            stat_msgs.push(message);
        }

        if positions.len() >= BATCHSIZE {
            postgres_prepare_tx_dynamic(&mut c, source, positions)?;
            positions = vec![];
        };
        if stat_msgs.len() >= BATCHSIZE {
            postgres_prepare_tx_static(&mut c, source, stat_msgs)?;
            stat_msgs = vec![];
        }
    }

    if !positions.is_empty() {
        postgres_prepare_tx_dynamic(&mut c, source, positions)?;
    }

    if !stat_msgs.is_empty() {
        postgres_prepare_tx_static(&mut c, source, stat_msgs)?;
    }

    let elapsed = start.elapsed();
    let f3 = filename.to_str().unwrap();
    let f4 = Path::new(f3);
    let fname = f4.file_name().unwrap().to_str().unwrap();
    let fname1 = format!("{:<1$}", fname, 64);
    let elapsed1 = format!(
        "elapsed: {:>1$}s",
        format!("{:.2 }", elapsed.as_secs_f32()),
        7
    );
    let rate1 = format!(
        "rate: {:>1$} msgs/s",
        format!("{:.0}", count as f32 / elapsed.as_secs_f32()),
        8
    );

    if verbose {
        println!(
            "{} count:{: >8}    {}    {}",
            fname1, count, elapsed1, rate1,
        );
    }

    Ok(())
}

/// progress database input from NOAA
#[cfg(feature = "sqlite")]
pub fn sqlite_decodemsgs_noaa_csv(
    dbpath: std::path::PathBuf,
    filename: std::path::PathBuf,
    source: &str,
    verbose: bool,
) -> Result<(), Error> {
    assert_eq!(&filename.extension().expect("getting file ext"), &"csv");

    let start = Instant::now();

    let mut reader = csv::Reader::from_reader(
        File::open(&filename).unwrap_or_else(|_| panic!("cannot open file {:?}", filename)),
    );
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();
    let mut count = 0;
    let mut static_seen: HashSet<u32> = HashSet::new();

    let mut c = get_db_conn(dbpath).expect("getting db conn");

    for row_option in reader.records(){
        count += 1;
        let row = match row_option {
            Ok(row) => row,
            Err(err) => {
                eprintln!("Skipping row due to CSV parsing error: {}", err);
                continue; // Skip the row and proceed with the next one
            }
        };
        let row_clone = row.clone();
        let epoch = match iso8601_2_epoch(row_clone.get(1).as_ref().unwrap()) {
            Some(epoch) => epoch as i32,
            None => {
                eprintln!("Skipping row due to invalid timestamp: {:?}", row_clone.get(1));
                return Ok(());
            }
        };
        let mmsi: u32 = match row.get(0).and_then(|m| m.parse::<u32>().ok()) {
            Some(mmsi) => mmsi,
            None => {
                eprintln!("Skipping row due to invalid MMSI: {:?}", row.get(0));
                continue; // Skip the row and move to the next one
            }
        };
        let payload_dynamic = VesselDynamicData {
            own_vessel: true,
            station: Station::BaseStation,
            ais_type: match row.get(16) {
                Some("A") => AisClass::ClassA,
                Some("B") => AisClass::ClassB,
                _ => AisClass::Unknown,
            },
            mmsi: mmsi,
            nav_status: NavigationStatus::new(row.get(11).unwrap().parse().unwrap_or_default()),
            rot: None,
            rot_direction: None,
            sog_knots: row.get(4).unwrap().parse::<f64>().ok(),
            high_position_accuracy: false,
            latitude: row.get(2).unwrap().parse().ok(),
            longitude: row.get(3).unwrap().parse().ok(),
            cog: row.get(5).unwrap().parse().ok(),
            heading_true: row.get(6).unwrap().parse().ok(),
            timestamp_seconds: 0, // enforced field
            positioning_system_meta: None,
            current_gnss_position: None,
            special_manoeuvre: None,
            raim_flag: false,
            class_b_unit_flag: None,
            class_b_display: None,
            class_b_dsc: None,
            class_b_band_flag: None,
            class_b_msg22_flag: None,
            class_b_mode_flag: None,
            class_b_css_flag: None,
            radio_status: None,
        };
        let message_dyn = VesselData {
            epoch: Some(epoch),
            payload: Some(ParsedMessage::VesselDynamicData(payload_dynamic)),
        };
        positions.push(message_dyn);

        if static_seen.insert(mmsi) {
            let payload_static = VesselStaticData {
                own_vessel: true,
                ais_type: match row.get(16) {
                    Some("A") => AisClass::ClassA,
                    Some("B") => AisClass::ClassB,
                    _ => AisClass::Unknown,
                },
                mmsi: mmsi,
                ais_version_indicator: 0, // NOAA does not contain such info but an u8 data type is enforced, we give default value 0 same with unsuccessful parsing result from Spire
                imo_number: row.get(8).unwrap().parse().ok(),
                call_sign: row.get(9).unwrap().parse().ok(),
                name: Some(row.get(7).unwrap_or("").to_string()),
                ship_type: ShipType::new(row.get(10).unwrap().parse().unwrap_or_default()),
                cargo_type: CargoType::new(row.get(15).unwrap().parse().unwrap_or_default()),
                equipment_vendor_id: None,
                equipment_model: None,
                equipment_serial_number: None,
                dimension_to_bow: None,
                dimension_to_stern: None,
                dimension_to_port: None,
                dimension_to_starboard: None,
                position_fix_type: None,
                eta: None,
                draught10: row.get(14).unwrap_or_default().parse().ok(),
                destination: None,
                mothership_mmsi: None,
            };
            let message_stat = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselStaticData(payload_static)),
            };
            stat_msgs.push(message_stat);
        }

        if positions.len() >= BATCHSIZE {
            let _d = sqlite_prepare_tx_dynamic(&mut c, source, positions);
            positions = vec![];
        };
        if stat_msgs.len() >= BATCHSIZE {
            let _s = sqlite_prepare_tx_static(&mut c, source, stat_msgs);
            stat_msgs = vec![];
        }
    }

    if !positions.is_empty() {
        let _d = sqlite_prepare_tx_dynamic(&mut c, source, positions);
    }
    if !stat_msgs.is_empty() {
        let _s = sqlite_prepare_tx_static(&mut c, source, stat_msgs);
    }

    let elapsed = start.elapsed();
    let f3 = filename.to_str().unwrap();
    let f4 = Path::new(f3);
    let fname = f4.file_name().unwrap().to_str().unwrap();
    let fname1 = format!("{:<1$}", fname, 64);
    let elapsed1 = format!(
        "elapsed: {:>1$}s",
        format!("{:.2 }", elapsed.as_secs_f32()),
        7
    );
    let rate1 = format!(
        "rate: {:>1$} msgs/s",
        format!("{:.0}", count as f32 / elapsed.as_secs_f32()),
        8
    );

    if verbose {
        println!(
            "{} count:{: >8}    {}    {}",
            fname1, count, elapsed1, rate1,
        );
    }

    Ok(())
}

/// progress database input from NOAA
#[cfg(feature = "postgres")]
pub fn postgres_decodemsgs_noaa_csv(
    connect_str: &str,
    filename: &std::path::PathBuf,
    source: &str,
    verbose: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(&filename.extension().expect("getting file ext"), &"csv");

    let start = Instant::now();

    let mut reader = csv::Reader::from_reader(File::open(filename)?);
    let mut stat_msgs = <Vec<VesselData>>::new();
    let mut positions = <Vec<VesselData>>::new();
    let mut count = 0;
    let mut static_seen: HashSet<u32> = HashSet::new();

    let mut c = get_postgresdb_conn(connect_str)?;

    for row_option in reader.records(){
        count += 1;
        let row = match row_option {
            Ok(row) => row,
            Err(err) => {
                eprintln!("Skipping row due to CSV parsing error: {}", err);
                continue; // Skip this row and proceed with the next one
            }
        };
        let row_clone = row.clone();
        let epoch = match iso8601_2_epoch(row_clone.get(1).as_ref().unwrap()) {
            Some(epoch) => epoch as i32,
            None => {
                eprintln!("Skipping row due to invalid timestamp: {:?}", row_clone.get(1));
                return Ok(());
            }
        };
        let mmsi: u32 = match row.get(0).and_then(|m| m.parse::<u32>().ok()) {
            Some(mmsi) => mmsi,
            None => {
                eprintln!("Skipping row due to invalid MMSI: {:?}", row.get(0));
                continue; // Skip this row and move to the next one
            }
        };
        
        let payload_dynamic = VesselDynamicData {
            own_vessel: true,
            station: Station::BaseStation,
//             ais_type: AisClass::new(row.get(16).unwrap().parse().unwrap_or_default()),
            ais_type: match row.get(16) {
                Some("A") => AisClass::ClassA,
                Some("B") => AisClass::ClassB,
                _ => AisClass::Unknown,
            },
            mmsi: mmsi,
            nav_status: NavigationStatus::new(row.get(11).unwrap().parse().unwrap_or_default()),
            rot: None,
            rot_direction: None,
            sog_knots: row.get(4).unwrap().parse::<f64>().ok(),
            high_position_accuracy: false,
            latitude: row.get(2).unwrap().parse().ok(),
            longitude: row.get(3).unwrap().parse().ok(),
            cog: row.get(5).unwrap().parse().ok(),
            heading_true: row.get(6).unwrap().parse().ok(),
            timestamp_seconds: 0, // enforced field
            positioning_system_meta: None,
            current_gnss_position: None,
            special_manoeuvre: None,
            raim_flag: false,
            class_b_unit_flag: None,
            class_b_display: None,
            class_b_dsc: None,
            class_b_band_flag: None,
            class_b_msg22_flag: None,
            class_b_mode_flag: None,
            class_b_css_flag: None,
            radio_status: None,
        };
        let message_dyn = VesselData {
            epoch: Some(epoch),
            payload: Some(ParsedMessage::VesselDynamicData(payload_dynamic)),
        };
        positions.push(message_dyn);

        if static_seen.insert(mmsi) {
            let payload_static = VesselStaticData {
                own_vessel: true,
                ais_type: match row.get(16) {
                    Some("A") => AisClass::ClassA,
                    Some("B") => AisClass::ClassB,
                    _ => AisClass::Unknown,
                },
                mmsi: mmsi,
                ais_version_indicator: 0,
                imo_number: row.get(8).unwrap().parse().ok(),
                call_sign: row.get(9).unwrap().parse().ok(),
                name: Some(row.get(7).unwrap_or("").to_string()),
                ship_type: ShipType::new(row.get(10).unwrap().parse().unwrap_or_default()),
                cargo_type: CargoType::new(row.get(15).unwrap().parse().unwrap_or_default()),
                equipment_vendor_id: None,
                equipment_model: None,
                equipment_serial_number: None,
                dimension_to_bow: None,
                dimension_to_stern: None,
                dimension_to_port: None,
                dimension_to_starboard: None,
                position_fix_type: None,
                eta: None,
                draught10: row.get(14).unwrap_or_default().parse().ok(),
                destination: None,
                mothership_mmsi: None,
            };
            let message_stat = VesselData {
                epoch: Some(epoch),
                payload: Some(ParsedMessage::VesselStaticData(payload_static)),
            };
            stat_msgs.push(message_stat);
        }

        if positions.len() >= BATCHSIZE {
            postgres_prepare_tx_dynamic(&mut c, source, positions)?;
            positions = vec![];
        };
        if stat_msgs.len() >= BATCHSIZE {
            postgres_prepare_tx_static(&mut c, source, stat_msgs)?;
            stat_msgs = vec![];
        }
    }

    if !positions.is_empty() {
        postgres_prepare_tx_dynamic(&mut c, source, positions)?;
    }

    if !stat_msgs.is_empty() {
        postgres_prepare_tx_static(&mut c, source, stat_msgs)?;
    }

    let elapsed = start.elapsed();
    let f3 = filename.to_str().unwrap();
    let f4 = Path::new(f3);
    let fname = f4.file_name().unwrap().to_str().unwrap();
    let fname1 = format!("{:<1$}", fname, 64);
    let elapsed1 = format!(
        "elapsed: {:>1$}s",
        format!("{:.2 }", elapsed.as_secs_f32()),
        7
    );
    let rate1 = format!(
        "rate: {:>1$} msgs/s",
        format!("{:.0}", count as f32 / elapsed.as_secs_f32()),
        8
    );

    if verbose {
        println!(
            "{} count:{: >8}    {}    {}",
            fname1, count, elapsed1, rate1,
        );
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {

    use super::{sqlite_decodemsgs_ee_csv, Error};
    use std::fs::File;
    use std::io::Write;
    pub use std::{
        fs::{create_dir_all, read_dir},
        io::{BufRead, BufReader},
        time::{Duration, Instant},
    };

    pub fn testingdata() -> Result<(), &'static str> {
        let c = r#"
MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Vessel_Name,Call_sign,IMO,Ship_Type,Dimension_to_Bow,Dimension_to_stern,Dimension_to_port,Dimension_to_starboard,Draught,Destination,AIS_version,Navigational_status,ROT,SOG,Accuracy,Longitude,Latitude,COG,Heading,Regional,Maneuver,RAIM_flag,Communication_flag,Communication_state,UTC_year,UTC_month,UTC_day,UTC_hour,UTC_minute,UTC_second,Fixing_device,Transmission_control,ETA_month,ETA_day,ETA_hour,ETA_minute,Sequence,Destination_ID,Retransmit_flag,Country_code,Functional_ID,Data,Destination_ID_1,Sequence_1,Destination_ID_2,Sequence_2,Destination_ID_3,Sequence_3,Destination_ID_4,Sequence_4,Altitude,Altitude_sensor,Data_terminal,Mode,Safety_text,Non-standard_bits,Name_extension,Name_extension_padding,Message_ID_1_1,Offset_1_1,Message_ID_1_2,Offset_1_2,Message_ID_2_1,Offset_2_1,Destination_ID_A,Offset_A,Increment_A,Destination_ID_B,offsetB,incrementB,data_msg_type,station_ID,Z_count,num_data_words,health,unit_flag,display,DSC,band,msg22,offset1,num_slots1,timeout1,Increment_1,Offset_2,Number_slots_2,Timeout_2,Increment_2,Offset_3,Number_slots_3,Timeout_3,Increment_3,Offset_4,Number_slots_4,Timeout_4,Increment_4,ATON_type,ATON_name,off_position,ATON_status,Virtual_ATON,Channel_A,Channel_B,Tx_Rx_mode,Power,Message_indicator,Channel_A_bandwidth,Channel_B_bandwidth,Transzone_size,Longitude_1,Latitude_1,Longitude_2,Latitude_2,Station_Type,Report_Interval,Quiet_Time,Part_Number,Vendor_ID,Mother_ship_MMSI,Destination_indicator,Binary_flag,GNSS_status,spare,spare2,spare3,spare4
"432448000","1","0","20211201_220415","80","66","","","","None","","","[28]","","","","","","","","","","","","7","0.0","3.5","0","-34.0796816667","14.69666","78.9","86.0","","0","0","","33286","","","","","","12","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"210335000","3","0","20211201_220450","450","66","","","","None","","","[28]","","","","","","","","","","","","0","0.0","13.9","0","152.655881667","-13.4423183333","164.2","164.0","","0","0","","11473","","","","","","47","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"374651000","1","0","20211201_220809","620","66","","","","None","","","[28]","","","","","","","","","","","","0","1.11600720834","13.3","0","-53.2738666667","8.10760166667","118.2","120.0","","0","0","","27680","","","","","","4","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"338789000","27","3","20211201_220853","630","66","","","","None","","","[16]","","","","","","","","","","","","0","","22.0","0","-77.68","27.9216666667","309.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"367404840","18","0","20211201_220919","460","66","","","","None","","","[28]","","","","","","","","","","","","","","1.7","0","170.216968333","17.7062233333","164.2","None","0","","0","1","393222","","","","","","14","","","","","","","","","","","","","","","","","","","","","","","","0","","","","","","","","","","","","","","","","","","","","","","1","0","1","1","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"993116106","21","0","20211201_220554","440","66","","","","None","","","[46]","","","","","0","0","0","0","","","","","","","0","-54.9975633333","8.97102","","","","","1","","","","","","","","60","1","","","","","","","","","","","","","","","","","","","","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","1","R TETHYS CABLE","0","0","1","","","","","","","","","","","","","","","","","","","","","","0","","",""
"211188900","27","3","20211201_221929","30","66","","","","None","","","[16]","","","","","","","","","","","","1","","0.0","1","10.4816666667","53.2416666667","None","","","","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"538007477","1","0","20211201_221951","530","66","","","","None","","","[28]","","","","","","","","","","","","8","0.0","1.9","1","-132.0437","54.24265","98.0","182.0","","0","0","","49155","","","","","","49","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"212352000","1","0","20211201_222012","680","66","","","","None","","","[28]","","","","","","","","","","","","0","-0.401762595001","17.4","0","-1.56493","-25.4976883333","124.4","125.0","","0","0","","49152","","","","","","6","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"205532090","27","3","20211201_224933","620","66","","","","None","","","[16]","","","","","","","","","","","","0","","0.0","1","0.321666666667","49.48","None","","","","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"355658000","1","0","20211201_225006","380","66","","","","None","","","[28]","","","","","","","","","","","","0","2.18737412834","18.2","0","166.964766667","37.01234","65.4","68.0","","0","0","","49153","","","","","","2","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"257302140","27","3","20211201_225835","510","66","","","","None","","","[16]","","","","","","","","","","","","0","","9.0","0","5.10833333333","62.0783333333","166.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"371785000","3","0","20211201_225858","100","66","","","","None","","","[28]","","","","","","","","","","","","2","None","0.5","0","179.640936667","-8.92283833333","241.2","226.0","","0","0","","0","","","","","","52","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"538009104","27","3","20211201_224522","880","66","","","","None","","","[16]","","","","","","","","","","","","0","","10.0","0","104.365","1.31666666667","47.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"354850000","1","0","20211201_224601","310","66","","","","None","","","[28]","","","","","","","","","","","","0","7.54420872835","15.6","1","154.368973333","28.7149916667","301.5","300.0","","0","0","","81923","","","","","","26","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"244750503","27","3","20211201_220230","770","66","","","","None","","","[16]","","","","","","","","","","","","15","","0.0","0","4.99666666667","51.87","None","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"503066000","27","3","20211201_220254","170","66","","","","None","","","[16]","","","","","","","","","","","","5","","0.0","0","115.008333333","-21.68","96.0","","","","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"601005500","1","0","20211201_222723","420","66","","","","None","","","[28]","","","","","","","","","","","","7","None","4.8","1","26.0865566667","-34.2307733333","39.8","None","","0","1","","33600","","","","","","22","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"244061000","27","3","20211201_222801","580","66","","","","None","","","[16]","","","","","","","","","","","","0","","12.0","1","9.89666666667","53.54","83.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"209213000","27","3","20211201_222817","600","66","","","","None","","","[16]","","","","","","","","","","","","1","","0.0","1","3.62166666667","51.415","152.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"372932000","27","3","20211201_222438","540","66","","","","None","","","[16]","","","","","","","","","","","","0","","19.0","0","103.06","1.45833333333","119.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"636019497","1","0","20211201_222511","570","66","","","","None","","","[28]","","","","","","","","","","","","0","0.0","14.1","1","-81.611365","18.193895","132.4","131.0","","0","0","","33084","","","","","","4","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"548335100","25","0","20211201_222935","810","66","","","","None","","","[28]","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","187496a8ab3d67cdf4b9c39642de4b2c","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","","","",""
"311000206","3","0","20211201_222953","630","66","","","","None","","","[28]","","","","","","","","","","","","1","0.0","0.1","0","32.0766816667","-28.895985","80.4","47.0","","0","0","","84938","","","","","","53","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"431602337","27","3","20211201_221229","630","66","","","","None","","","[16]","","","","","","","","","","","","1","","0.0","0","133.568333333","33.5116666667","164.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"538090451","1","0","20211201_221251","500","66","","","","None","","","[28]","","","","","","","","","","","","0","-6.42820152001","15.7","0","4.88901","-13.3330366667","315.1","315.0","","0","0","","2249","","","","","","46","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"477723200","1","0","20211201_221318","410","66","","","","None","","","[28]","","","","","","","","","","","","0","3.61586335501","17.5","0","153.144901667","-31.9049866667","7.9","8.0","","0","0","","27700","","","","","","11","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"232015624","27","3","20211201_220742","530","66","","","","None","","","[16]","","","","","","","","","","","","5","","0.0","1","-157.951666667","21.3533333333","187.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"413510730","27","3","20211201_220816","100","66","","","","None","","","[16]","","","","","","","","","","","","0","","12.0","0","113.601666667","22.0516666667","215.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"226105000","27","3","20211201_223743","610","66","","","","None","","","[16]","","","","","","","","","","","","0","","3.0","0","-9.64166666667","57.5583333333","181.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"255806512","1","0","20211201_223805","390","66","","","","None","","","[28]","","","","","","","","","","","","0","0.0","13.2","0","-113.7041","22.4757","119.0","120.0","","0","0","","27796","","","","","","55","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"373076000","3","0","20211201_223233","740","66","","","","None","","","[28]","","","","","","","","","","","","0","0.0","10.9","1","-169.435366667","51.4281866667","79.6","65.0","","0","0","","73677","","","","","","23","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"414278590","27","3","20211201_223253","990","66","","","","None","","","[16]","","","","","","","","","","","","15","","0.0","1","121.085","33.2966666667","None","","","","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"566983000","27","3","20211201_224423","660","66","","","","None","","","[16]","","","","","","","","","","","","3","","0.0","0","-2.93666666667","4.53833333333","4.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"339202000","27","3","20211201_224500","920","66","","","","None","","","[16]","","","","","","","","","","","","5","","0.0","0","-80.0483333333","26.7133333333","130.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"367657270","27","3","20211201_225922","130","66","","","","None","","","[16]","","","","","","","","","","","","0","","10.0","0","168.18","31.7133333333","270.0","","","","0","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"311000387","1","0","20211201_225958","850","66","","","","None","","","[28]","","","","","","","","","","","","0","-1.60705038","14.3","0","159.615465","34.2145016667","73.5","81.0","","0","0","","114691","","","","","","58","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"477045500","1","0","20211201_223931","847","66","","","","None","","","[28]","","","","","","","","","","","","0","-1.11600720834","0.6","0","151.945833333","-33.156165","243.2","208.0","","0","0","","114738","","","","","","19","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"503774000","1","0","20211201_224258","539","66","","","","None","","","[28]","","","","","","","","","","","","0","None","9.0","0","153.270383333","-27.2147183333","34.3","None","","0","0","","81937","","","","","","2","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"354227000","1","0","20211201_222117","480","66","","","","None","","","[28]","","","","","","","","","","","","0","0.0","11.4","0","-19.3662866667","19.727565","24.2","26.0","","0","0","","27732","","","","","","12","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"735058312","18","3","20211201_222142","950","66","","","","None","","","[28]","","","","","","","","","","","","","","9.4","1","-90.3051516667","-0.487873333333","109.2","None","0","","1","1","0","","","","","","40","","","","","","","","","","","","","","","","","","","","","","","","0","","","","","","","","","","","","","","","","","","","","","","1","0","1","1","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"412421067","24","0","20211201_221838","450","66","","","","None","","","[28]","","BZW5J","","30","35","25","5","4","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","1","U/=2A'I","","","","","38","","",""
"269057587","27","3","20211201_221907","340","66","","","","None","","","[16]","","","","","","","","","","","","0","","7.0","1","5.41333333333","51.93","308.0","","","","1","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","0","","",""
"538007971","3","0","20211201_222930","231","66","","","","None","","","[28]","","","","","","","","","","","","1","0.0","0.1","0","-15.4054216667","28.10836","330.1","39.0","","0","0","","86778","","","","","","31","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","",""
"81562826","24","0","20211201_190606","410","66","","","","None","","","[27]","CI012-16","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","0","","","","","","","","",""
"#;

        if create_dir_all("testdata/").is_ok() {
            let mut output = File::create("testdata/testingdata.csv").unwrap();
            let _ = write!(output, "{}", c);
            Ok(())
        } else {
            Err("cant create testing data dir!")
        }
    }

    #[test]
    pub fn test_csv() -> Result<(), Error> {
        let _ = testingdata();

        let fpath = std::path::PathBuf::from("testdata/testingdata.csv");

        let _ = sqlite_decodemsgs_ee_csv(
            &std::path::Path::new("testdata/test.db").to_path_buf(),
            &fpath,
            "TESTDATA",
            true,
        );

        Ok(())
    }
}
