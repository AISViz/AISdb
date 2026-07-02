use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::net::TcpStream;

use aisdb_lib::db::sql_from_file;

use chrono::{DateTime, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use geo::SimplifyVwIdx;
use geo_types::{Coord, LineString};
use postgres::{Client, Portal, Row, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json, Value as JsValue};
use tungstenite::{accept, Message};

macro_rules! zip {
    ($x: expr) => ($x);
    ($x: expr, $($y: expr), +) => (
        $x.iter().zip(
            zip!($($y), +))
        )
}

/// coordinate bounding box requested by user
#[derive(Deserialize, Debug)]
struct Boundary {
    x0: f32,
    x1: f32,
    y0: f32,
    y1: f32,
}

/// client request format
#[derive(Deserialize)]
struct Request {
    msgtype: String,
    start: Option<i32>,
    end: Option<i32>,
    area: Option<Boundary>,
}

/// requests are converted into a query with a time range and coordinate bounding box
struct QueryTracks {
    start: i32,
    end: i32,
    month_strings: Vec<String>,
    area: Boundary,
}

/// server response to track vectors request.
/// zone geometry also uses this format,
/// with an empty time vector (only x and y for coordinates).
#[derive(Serialize)]
pub struct Response<'a> {
    pub msgtype: String,
    pub x: &'a Vec<TrackData>,
    pub y: &'a Vec<TrackData>,
    pub t: &'a Vec<TrackData>,
    pub meta: HashMap<String, String>,
}

/// server response to valid date range as a UNIX timestamp
#[derive(Serialize)]
pub struct DaterangeResponse {
    pub msgtype: String,
    pub start: i32,
    pub end: i32,
}

/// Track vector data from postgres queries may contain these types
#[derive(Debug, Serialize, PartialEq, PartialOrd, Clone)]
#[serde(untagged)]
pub enum TrackData {
    I(i32),
    F(f64),
    S(String),
}

impl TrackData {
    /// convert postgres F4 to f64
    fn as_float(&self) -> f64 {
        match self {
            TrackData::F(f) => *f,
            other => panic!("expected float track data, got {:?}", other),
        }
    }
}

/// tracks are defined as a pair of hashmaps, for metadata and vector data columns
#[derive(Debug, Serialize)]
pub struct Track {
    pub meta: HashMap<String, String>,
    pub vectors: HashMap<String, Vec<TrackData>>,
}

/// Each track consists of two HashMaps:
/// Metadata strings, and vectored integers/floats for
/// positional data. All keys are Strings
impl Track {
    pub fn new() -> Track {
        let mut t = Track {
            meta: HashMap::new(),
            vectors: HashMap::new(),
        };
        t.vectors.insert("time".to_string(), Vec::new());
        t.vectors.insert("longitude".to_string(), Vec::new());
        t.vectors.insert("latitude".to_string(), Vec::new());
        t.vectors.insert("sog".to_string(), Vec::new());
        t.vectors.insert("cog".to_string(), Vec::new());
        t
    }
}

impl Default for Track {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_utctime(timestamp: i32) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    DateTime::from_timestamp(timestamp as i64, 0)
        .ok_or_else(|| format!("invalid epoch timestamp: {}", timestamp).into())
}

/// Parses a client request for track vectors, and returns parameters for database query
fn parse_request(req: Request) -> Result<QueryTracks, Box<dyn std::error::Error>> {
    let start = parse_utctime(req.start.ok_or("missing start time")?)?;
    let end = parse_utctime(req.end.ok_or("missing end time")?)?;

    let mut cursor = start;
    let mut month_strings: Vec<String> = vec![];
    while cursor < end {
        let month_str = cursor.format("%Y%m").to_string();
        if !month_strings.contains(&month_str) {
            month_strings.push(month_str);
        }
        cursor += chrono::Duration::days(21);
    }
    let month_str = end.format("%Y%m").to_string();
    if !month_strings.contains(&month_str) {
        month_strings.push(month_str);
    }
    let qry = QueryTracks {
        start: start.timestamp() as i32,
        end: end.timestamp() as i32,
        month_strings,
        area: req.area.ok_or("missing query boundary args")?,
    };
    if qry.start >= qry.end {
        Err("invalid time range".into())
    } else if qry.area.x0 >= qry.area.x1 {
        Err("invalid latitude range".into())
    } else if qry.area.y0 >= qry.area.y1 {
        Err("invalid longitude range".into())
    } else {
        Ok(qry)
    }
}

fn append_row(track: &mut Track, r: &Row) -> Result<(), Box<dyn std::error::Error>> {
    for col in r.columns().iter().map(|c| c.name().to_string()) {
        match col.as_str() {
            // static integers
            "mmsi" | "imo" | "dim_bow" | "dim_star" | "dim_stern" | "dim_port" | "ship_type"
            | "gross_tonnage" | "summer_dwt" | "year_built" => {
                if let Entry::Vacant(entry) = track.meta.entry(col.clone()) {
                    let value: Option<i32> = r.try_get(col.as_str())?;
                    entry.insert(value.map(|v| v.to_string()).unwrap_or_default());
                }
            }

            // static strings
            "vessel_name"
            | "vessel_name2"
            | "ship_type_txt"
            | "vesseltype_generic"
            | "vesseltype_detailed"
            | "length_breadth" => {
                let value: Option<String> = r.try_get(col.as_str())?;
                track.meta.insert(col, value.unwrap_or_default());
            }

            // dynamic integers
            "time" => {
                let v = TrackData::I(r.try_get(col.as_str())?);
                track
                    .vectors
                    .get_mut(&col)
                    .ok_or("missing time vector")?
                    .push(v);
            }

            // dynamic floats
            "longitude" | "latitude" | "cog" | "sog" => {
                let f: f32 = r.try_get(col.as_str())?;
                track
                    .vectors
                    .get_mut(&col)
                    .ok_or("missing vector column")?
                    .push(TrackData::F(f as f64));
            }
            other => return Err(format!("unhandled track vector: {}", other).into()),
        }
    }
    Ok(())
}

/// Collects vessel track vectors from the result of a database query portal.
/// Relies on the assumption that query results are sorted by ID and time.
pub fn collect_tracks(
    portal: Portal,
    mut tx: Transaction<'_>,
) -> Result<Vec<Track>, Box<dyn std::error::Error>> {
    const CHUNKSIZE: i32 = 50000;
    let mut tracks: Vec<Track> = Vec::new();
    let mut current_track = Track::new();
    let mut current_mmsi = 0;

    let mut rows: VecDeque<Row> = VecDeque::from(tx.query_portal(&portal, CHUNKSIZE)?);
    while !rows.is_empty() {
        while let Some(r) = rows.pop_front() {
            let thismmsi: i32 = r.try_get("mmsi")?;
            if current_mmsi == 0 {
                current_mmsi = thismmsi;
            } else if thismmsi != current_mmsi {
                current_mmsi = thismmsi;
                tracks.push(current_track);
                current_track = Track::new();
            } else {
                append_row(&mut current_track, &r)?;
            }
        }
        rows = VecDeque::from(tx.query_portal(&portal, CHUNKSIZE)?);
    }
    if !current_track
        .vectors
        .get("time")
        .ok_or("missing time vector")?
        .is_empty()
    {
        tracks.push(current_track);
    }
    println!("yielded {} track vectors", tracks.len());
    Ok(tracks)
}

fn query_validrange(pg: &mut Client) -> Result<(i32, i32), Box<dyn std::error::Error>> {
    let mut sql = "SELECT table_name FROM information_schema.tables".to_string();
    sql.push_str(" WHERE table_schema='public' AND table_type='BASE TABLE'");
    sql.push_str(" AND table_name LIKE '%_dynamic'");
    sql.push_str(" ORDER BY table_name ASC");
    let tables = pg.query(&sql, &[])?;
    if tables.is_empty() {
        return Err("no dynamic tables found in database".into());
    }
    let start_table: String = tables[0].try_get(0)?;
    let end_table: String = tables[tables.len() - 1].try_get(0)?;
    let start: i32 = pg
        .query_one(
            &format!("SELECT time FROM {} ORDER BY time ASC LIMIT 1", start_table),
            &[],
        )?
        .try_get(0)?;
    let end: i32 = pg
        .query_one(
            &format!("SELECT time FROM {} ORDER BY time DESC LIMIT 1", end_table),
            &[],
        )?
        .try_get(0)?;
    if start > end {
        Err("Invalid range (start > end)".into())
    } else {
        Ok((start, end))
    }
}

fn query_metadata(
    pg: &mut Client,
    start: i32,
    end: i32,
) -> Result<Vec<JsValue>, Box<dyn std::error::Error>> {
    let start = parse_utctime(start)?;
    let end = parse_utctime(end)?;
    let mut cursor = start;
    let mut month_strings: Vec<String> = vec![];
    while cursor <= end {
        let month_str = cursor.format("%Y%m").to_string();
        if !month_strings.contains(&month_str) {
            month_strings.push(month_str);
        }
        cursor += chrono::Duration::days(21);
    }

    // perform UNION of data request for each monthly table
    let sql: String = sql_from_file("select_static_join_webdata.sql").to_string();
    let sql_union = month_strings
        .iter()
        .map(|m| sql.replace("{}", m))
        .collect::<Vec<String>>()
        .join("UNION\n");
    let stmt = pg.prepare(&sql_union)?;
    let mut tx = pg.transaction()?;
    let portal = tx.bind(&stmt, &[])?;

    const CHUNKSIZE: i32 = 2048;
    let mut messages: Vec<JsValue> = Vec::new();
    let mut rows = tx.query_portal(&portal, CHUNKSIZE)?;
    while !rows.is_empty() {
        for r in rows {
            let mut h: HashMap<String, TrackData> = HashMap::new();
            h.insert(
                "msgtype".to_string(),
                TrackData::S("vesselinfo".to_string()),
            );
            for col in r.columns() {
                match col.name() {
                    "mmsi" | "imo" | "gross_tonnage" | "summer_dwt" | "year_built" => {
                        if let Some(v) = r.try_get::<&str, Option<i32>>(col.name())? {
                            h.insert(col.name().to_string(), TrackData::I(v));
                        }
                    }
                    string_column => {
                        if let Some(v) = r.try_get::<&str, Option<String>>(col.name())? {
                            h.insert(string_column.to_string(), TrackData::S(v));
                        }
                    }
                }
            }
            messages.push(json!(h));
        }
        rows = tx.query_portal(&portal, CHUNKSIZE)?;
    }
    println!("sending {} info messages to client", messages.len());

    let mut done = HashMap::new();
    done.insert("msgtype".to_string(), "doneMetadata".to_string());
    messages.push(json!(done));
    Ok(messages)
}

/// converts a request into SQL query string and list of parameters
fn query_dynamic_tables(
    req: Request,
    pg: &mut Client,
) -> Result<Vec<Track>, Box<dyn std::error::Error>> {
    let mut sql: String = sql_from_file("cte_dynamic_clusteredidx.sql").to_string();
    let mut filter: String = "    d.time >= $1\n    and d.time <= $2".to_string();
    let s: String = "\n    and d.longitude >= $3".to_string()
        + "\n    and d.longitude <= $4"
        + "\n    and d.latitude >= $5"
        + "\n    and d.latitude <= $6";
    filter += &s;
    sql.push_str(&filter);

    let qry = parse_request(req)?;

    // perform UNION of data request for each monthly table
    let sql_union = qry
        .month_strings
        .iter()
        .map(|m| sql.replace("{}", m))
        .collect::<Vec<String>>()
        .join("\nUNION\n")
        + "\n  ORDER BY 1, 2";
    #[cfg(debug_assertions)]
    println!("{}", sql_union);
    let stmt = pg.prepare(&sql_union)?;
    let mut tx = pg.transaction()?;
    let area = qry.area;
    let portal = tx.bind(
        &stmt,
        &[&qry.start, &qry.end, &area.x0, &area.x1, &area.y0, &area.y1],
    )?;

    collect_tracks(portal, tx)
}

/// converts a request into SQL query string and list of parameters
fn query_merged_tables(
    req: Request,
    pg: &mut Client,
) -> Result<Vec<Track>, Box<dyn std::error::Error>> {
    let mut sql_dynamic: String = sql_from_file("cte_dynamic_clusteredidx.sql").to_string();
    let sql_static = sql_from_file("cte_static.sql");
    let sql_coarsetype = sql_from_file("cte_coarsetype.sql");
    let sql_leftjoin = sql_from_file("select_merged_all.sql");
    let mut filter: String = "    d.time >= $1\n    and d.time <= $2".to_string();
    let s: String = "\n    and d.longitude >= $3".to_string()
        + "\n    and d.longitude <= $4"
        + "\n    and d.latitude >= $5"
        + "\n    and d.latitude <= $6";
    filter += &s;
    sql_dynamic.push_str(&filter);

    let sql = format!(
        "WITH dynamic_{{}} AS (\n{}\n),\nstatic_{{}} AS ({}),\n{}{} \nORDER BY 1,2",
        sql_dynamic, sql_static, sql_coarsetype, sql_leftjoin
    );

    let qry = parse_request(req)?;
    let area = qry.area;

    // perform UNION of data request for each monthly table
    let sql_union = qry
        .month_strings
        .iter()
        .map(|m| sql.replace("{}", m))
        .collect::<Vec<String>>()
        .join("\nUNION\n");
    #[cfg(debug_assertions)]
    println!(
        "{}\n{:?} {:?}",
        sql_union,
        &[&qry.start, &qry.end],
        &[&area.x0, &area.x1, &area.y0, &area.y1]
    );
    let stmt = pg.prepare(&sql_union)?;
    let mut tx = pg.transaction()?;
    let portal = tx.bind(
        &stmt,
        &[&qry.start, &qry.end, &area.x0, &area.x1, &area.y0, &area.y1],
    )?;

    collect_tracks(portal, tx)
}

pub fn compress_string_zlib(s: String) -> Result<Vec<u8>, std::io::Error> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(s.as_bytes())?;
    e.finish()
}

/// rounds float values in a track vector to 5 decimal places.
/// this is used to help with JSON message compression
fn round_floats(tracks: Vec<Track>, precision: f64) -> Vec<Track> {
    let multiplier = (1.0 / precision).round();
    tracks
        .into_iter()
        .map(|mut track| {
            for column_data in track.vectors.values_mut() {
                let is_float = matches!(column_data.first(), Some(TrackData::F(_)));
                if is_float {
                    for v in column_data.iter_mut() {
                        *v = TrackData::F((v.as_float() * multiplier).round() / multiplier);
                    }
                }
            }
            track
        })
        .collect()
}

fn compress_geometry_vectors(tracks: Vec<Track>, precision: f64) -> Vec<Track> {
    tracks
        .into_iter()
        .map(|track| {
            let coords: Vec<Coord> = zip!(
                track.vectors.get("longitude").expect("longitude vector"),
                track.vectors.get("latitude").expect("latitude vector")
            )
            .map(|(xx, yy)| Coord {
                x: xx.as_float(),
                y: yy.as_float(),
            })
            .collect();
            let count_orig = coords.len();

            let mut idx_deque = VecDeque::from_iter(LineString(coords).simplify_vw_idx(&precision));
            let mut mask: Vec<bool> = Vec::with_capacity(count_orig);
            for i in 0..count_orig {
                if idx_deque.front() == Some(&i) {
                    mask.push(true);
                    idx_deque.pop_front();
                } else {
                    mask.push(false);
                }
            }

            let mut newtrack = Track::new();
            for (col, values) in &track.vectors {
                let filtered: Vec<TrackData> = values
                    .iter()
                    .zip(mask.iter())
                    .filter(|(_v, m)| **m)
                    .map(|(v, _m)| v.clone())
                    .collect();
                newtrack.vectors.insert(col.clone(), filtered);
            }
            newtrack.meta = track.meta;
            newtrack
        })
        .collect()
}

/// Prepare client responses containing polygon geometry vectors.
/// Currently, there are only two polygons
fn default_zones() -> Vec<JsValue> {
    let mut zones = Vec::new();
    let mut z1 = Response {
        msgtype: "zone".to_string(),
        x: &Vec::from(
            [-63.554560, -63.554560, -63.552039, -63.552039, -63.554560].map(TrackData::F),
        ),
        y: &Vec::from(
            [44.4677006, 44.4694993, 44.4694993, 44.4677006, 44.4677006].map(TrackData::F),
        ),
        t: &Vec::new(),
        meta: HashMap::new(),
    };
    z1.meta
        .insert("name".to_string(), "AIS Station 2: NRC".to_string());
    zones.push(json!(z1));

    let mut z2 = Response {
        msgtype: "zone".to_string(),
        x: &Vec::from(
            [-63.588463, -63.588463, -63.585936, -63.585936, -63.588463].map(TrackData::F),
        ),
        y: &Vec::from(
            [44.6365006, 44.6382993, 44.6382993, 44.6365006, 44.6365006].map(TrackData::F),
        ),
        t: &Vec::new(),
        meta: HashMap::new(),
    };
    z2.meta
        .insert("name".to_string(), "AIS Station 1: Dalhousie".to_string());
    zones.push(json!(z2));

    zones
}

pub fn handle_client(
    downstream: TcpStream,
    pg: &mut Client,
) -> Result<(), Box<dyn std::error::Error>> {
    fn write_response(
        tracks: Vec<Track>,
        websocket: &mut tungstenite::WebSocket<TcpStream>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut count = 0;
        for track in tracks {
            let response = json!(Response {
                msgtype: "track_vector".to_string(),
                x: track.vectors.get("longitude").ok_or("missing longitude")?,
                y: track.vectors.get("latitude").ok_or("missing latitude")?,
                t: track.vectors.get("time").ok_or("missing time")?,
                meta: track.meta.clone(),
            });
            websocket.send(Message::Binary(response.to_string().into()))?;
            count += 1;
        }

        let status_response = format!(
            "{{\"msgtype\":\"done\", \"status\":\"Done. Count: {}\"}}",
            count
        );
        websocket.send(Message::Binary(status_response.into()))?;
        Ok(())
    }

    // timeouts and TLS are handled by gateway
    downstream.set_read_timeout(None)?;
    downstream.set_write_timeout(None)?;

    let remote_hostname = downstream.peer_addr()?;
    let mut websocket =
        accept(downstream).map_err(|e| format!("websocket handshake failed: {}", e))?;

    // loop will await client requests and respond accordingly
    loop {
        println!("awaiting message from {} ...", remote_hostname);

        let request_data = websocket.read()?;
        match request_data {
            Message::Ping(_) => {
                websocket.send(Message::Pong(vec![0u8].into()))?;
                continue;
            }
            Message::Close(_) => {
                println!("received close message from client, exiting...");
                return Ok(());
            }
            _ => {}
        }
        let req_txt = request_data.to_text()?;
        println!("got request from {}: {}", remote_hostname, req_txt);
        let req = from_str::<Request>(req_txt)?;

        match req.msgtype.as_str() {
            "track_vectors" => {
                let tracks = query_dynamic_tables(req, pg)?;
                let tracks = compress_geometry_vectors(tracks, 0.0001);
                let tracks = round_floats(tracks, 0.0001);
                write_response(tracks, &mut websocket)?;
            }

            "track_vectors_extra" => {
                let tracks = query_merged_tables(req, pg)?;
                let tracks = compress_geometry_vectors(tracks, 0.0001);
                let tracks = round_floats(tracks, 0.0001);
                write_response(tracks, &mut websocket)?;
            }

            // Returns the timerange where data exists in the database
            "validrange" => {
                let (start, end) = query_validrange(pg)?;
                let response = json!(DaterangeResponse {
                    msgtype: "validrange".to_string(),
                    start,
                    end
                });
                println!("sending date range response {}", response);
                websocket.send(Message::Binary(response.to_string().into()))?;
            }

            "meta" => {
                let (start, end) = query_validrange(pg)?;
                println!("sending vessel metadata...");
                for obj in query_metadata(pg, start, end)? {
                    websocket.send(Message::Binary(obj.to_string().into()))?;
                }
            }

            // Draw geojson polygons on client
            "zones" => {
                for zone in default_zones() {
                    websocket.send(Message::binary(zone.to_string()))?;
                }
                websocket.send(Message::Binary(
                    "{\"msgtype\":\"doneZones\"}".as_bytes().to_vec(),
                ))?;
            }

            unknown_query_type => {
                return Err(format!("unknown query type: {}", unknown_query_type).into());
            }
        }
    }
}
