use std::collections::{HashMap, VecDeque};
use std::convert::From;
use std::io::Write;
use std::net::TcpStream;
pub use std::ops::{Generator, GeneratorState, IndexMut};
use std::pin::Pin;

use aisdb_lib::db::sql_from_file;

use chrono::{DateTime, NaiveDateTime, Utc};
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
            _ => {
                panic!()
            }
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

pub struct GeneratorIteratorAdapter<G>(Pin<Box<G>>);
impl<G> GeneratorIteratorAdapter<G>
where
    G: Generator<Return = ()>,
{
    fn new(gen: G) -> Self {
        Self(Box::pin(gen))
    }
}

impl<G> Iterator for GeneratorIteratorAdapter<G>
where
    G: Generator<Return = ()>,
{
    type Item = G::Yield;

    fn next(&mut self) -> Option<Self::Item> {
        match self.0.as_mut().resume(()) {
            GeneratorState::Yielded(x) => Some(x),
            GeneratorState::Complete(_) => None,
        }
    }
}

fn parse_utctime(timestamp: i32) -> Result<DateTime<Utc>, chrono::format::ParseError> {
    Ok(DateTime::from_utc(
        NaiveDateTime::from_timestamp_opt(timestamp as i64, 0).expect("parsing epoch time"),
        Utc,
    ))
}

/// Parses a client request for track vectors, and returns parameters for database query
fn parse_request(req: Request) -> Result<QueryTracks, Box<dyn std::error::Error>> {
    let start = parse_utctime(req.start.unwrap()).expect("parsing start time");
    let end = parse_utctime(req.end.unwrap()).expect("parsing end time");

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
        area: req.area.expect("retrieving query boundary args"),
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

/// Generates vessel track vectors from the result of a database query portal
pub fn track_generator(
    portal: Portal,
    mut tx: Transaction<'_>,
) -> impl Generator<Yield = Track, Return = ()> + '_ {
    let mut count = 0;
    move || {
        let mut current_track = Track::new();
        let mut current_mmsi = 0;

        // iterate through rows fetching 50k at a time
        let chunksize = 50000;
        let mut rows: VecDeque<Row> = VecDeque::from(tx.query_portal(&portal, chunksize).unwrap());
        assert!(!rows.is_empty());
        while !rows.is_empty() {
            // iterate through rows, appending the results to current_track,
            // continue iterating until the mmsi changes.
            // this code relies on the assumption that query results are sorted by ID and time
            while !rows.is_empty() {
                let r: Row = rows.pop_front().unwrap();
                let thismmsi: i32 = r.get("mmsi");
                if current_mmsi == 0 {
                    #[cfg(debug_assertions)]
                    assert!(current_track
                        .vectors
                        .get("time")
                        .expect("get time")
                        .is_empty());
                    current_mmsi = thismmsi;
                } else if thismmsi != current_mmsi {
                    current_mmsi = r.get("mmsi");
                    yield current_track;
                    count += 1;
                    current_track = Track::new();
                } else {
                    for col in r.columns().iter().map(|r| r.name().to_string()) {
                        match col.as_str() {
                            // static integers
                            "mmsi" | "imo" | "dim_bow" | "dim_star" | "dim_stern" | "dim_port"
                            | "ship_type" | "gross_tonnage" | "summer_dwt" | "year_built" => {
                                if !current_track.meta.contains_key(&col) {
                                    let value: Option<i32> = r.get(col.as_str());
                                    if let Some(val) = value {
                                        current_track
                                            .meta
                                            .insert(col.to_string(), format!("{}", val));
                                    } else {
                                        current_track.meta.insert(col.to_string(), "".to_string());
                                    }
                                }
                            }

                            // static strings
                            "vessel_name"
                            | "vessel_name2"
                            | "ship_type_txt"
                            | "vesseltype_generic"
                            | "vesseltype_detailed"
                            | "length_breadth" => {
                                let value: Option<String> = r.get(col.as_str());
                                if let Some(val) = value {
                                    current_track.meta.insert(col.to_string(), val);
                                } else {
                                    current_track.meta.insert(col.to_string(), "".to_string());
                                }
                            }

                            // dynamic integers
                            "time" => {
                                let v = TrackData::I(r.get(col.as_str()));
                                current_track.vectors.get_mut(&col).unwrap().push(v);
                                debug_assert!(!current_track.vectors.get(&col).unwrap().is_empty())
                            }

                            // dynamic floats
                            "longitude" | "latitude" | "cog" | "sog" => {
                                let f: f32 = r.get(col.as_str());
                                let v = TrackData::F(f as f64);
                                current_track.vectors.get_mut(&col).unwrap().push(v);
                            }
                            _other => {
                                panic!("unhandled track vector: {}", _other)
                            }
                        }
                    }
                }
            }
            rows = VecDeque::from(tx.query_portal(&portal, chunksize).unwrap());
        }
        if !current_track.vectors.get("time").unwrap().is_empty() {
            yield current_track;
            count += 1;
        }
        println!("yielded {} track vectors", count);
    }
}

fn query_validrange(pg: &mut Client) -> Result<(i32, i32), Box<dyn std::error::Error>> {
    let mut sql = "SELECT table_name FROM information_schema.tables".to_string();
    sql.push_str(" WHERE table_schema='public' AND table_type='BASE TABLE'");
    sql.push_str(" AND table_name LIKE '%_dynamic'");
    sql.push_str(" ORDER BY table_name ASC");
    let tables = pg.query(&sql, &[])?;
    if tables.is_empty() {
        panic!("Empty database!");
    }
    let start_table: String = tables[0].get(0);
    let end_table: String = tables[tables.len() - 1].get(0);
    let start: i32 = pg
        .query_one(
            &format!("SELECT time FROM {} ORDER BY time ASC LIMIT 1", start_table),
            &[],
        )?
        .get(0);
    let end: i32 = pg
        .query_one(
            &format!("SELECT time FROM {} ORDER BY time DESC LIMIT 1", end_table),
            &[],
        )?
        .get(0);
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
) -> GeneratorIteratorAdapter<impl Generator<Yield = JsValue, Return = ()> + '_> {
    let start: DateTime<Utc> = DateTime::from_utc(
        NaiveDateTime::from_timestamp_opt(start.into(), 0).unwrap(),
        Utc,
    );
    let end: DateTime<Utc> = DateTime::from_utc(
        NaiveDateTime::from_timestamp_opt(end.into(), 0).unwrap(),
        Utc,
    );
    let mut cursor = start;
    let mut month_strings: Vec<String> = vec![];
    while cursor <= end {
        let month_str = cursor.format("%Y%m").to_string();
        if !month_strings.contains(&month_str) {
            month_strings.push(month_str);
        }
        cursor += chrono::Duration::days(21);
    }

    #[cfg(debug_assertions)]
    assert!(!month_strings.is_empty());

    // perform UNION of data request for each monthly table
    let sql: String = sql_from_file("select_static_join_webdata.sql").to_string();
    let sql_union = month_strings
        .iter()
        .map(|m| sql.replace("{}", m))
        .collect::<Vec<String>>()
        .join("UNION\n")
        //+ "\nORDER BY 1"
        ;
    let stmt = pg.prepare(&sql_union).unwrap();
    //let stmt = pg.prepare(&sql).unwrap();
    let mut tx = pg.transaction().unwrap();
    let portal = tx.bind(&stmt, &[]).unwrap();

    let mut c = 0;

    GeneratorIteratorAdapter::new(move || {
        let chunksize = 2048;
        let mut rows = tx.query_portal(&portal, chunksize).unwrap();
        while !rows.is_empty() {
            for r in rows {
                c += 1;
                let mut h: HashMap<String, TrackData> = HashMap::new();
                h.insert(
                    "msgtype".to_string(),
                    TrackData::S("vesselinfo".to_string()),
                );
                for col in r.columns() {
                    match col.name() {
                        "mmsi" | "imo" | "gross_tonnage" | "summer_dwt" | "year_built" => {
                            if let Some(v) = r.get::<&str, std::option::Option<i32>>(col.name()) {
                                h.insert(col.name().to_string(), TrackData::I(v));
                            }
                        }
                        string_column => {
                            if let Some(v) = r.get::<&str, std::option::Option<String>>(col.name())
                            {
                                h.insert(string_column.to_string(), TrackData::S(v));
                            }
                        }
                    }
                }
                yield json!(h);
            }
            rows = tx.query_portal(&portal, chunksize).unwrap();
            //break;
        }
        assert!(tx.query_portal(&portal, chunksize).unwrap().is_empty());
        println!("sent {} info messages to client", c);

        let mut done = HashMap::new();
        done.insert("msgtype".to_string(), "doneMetadata".to_string());
        yield json!(done)
    })
}

/// converts a request into SQL query string and list of parameters
fn query_dynamic_tables(
    req: Request,
    pg: &mut Client,
) -> GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()> + '_> {
    // SQL code: load dynamic messages from dynamic message tables
    let mut sql: String = sql_from_file("cte_dynamic_clusteredidx.sql").to_string();
    let mut filter: String = "    d.time >= $1\n    and d.time <= $2".to_string();
    let s: String = "\n    and d.longitude >= $3".to_string()
        + "\n    and d.longitude <= $4"
        + "\n    and d.latitude >= $5"
        + "\n    and d.latitude <= $6";
    filter += &s;
    sql.push_str(&filter);

    // parse client request into query parameters
    let qry = parse_request(req).expect("parsing request params");

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
    let stmt = pg.prepare(&sql_union).unwrap();
    let mut tx = pg.transaction().unwrap();
    let area = qry.area;
    let portal = tx
        .bind(
            &stmt,
            &[&qry.start, &qry.end, &area.x0, &area.x1, &area.y0, &area.y1],
        )
        .unwrap();

    GeneratorIteratorAdapter::new(track_generator(portal, tx))
}

/// converts a request into SQL query string and list of parameters
fn query_merged_tables(
    req: Request,
    pg: &mut Client,
) -> GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()> + '_> {
    // SQL code: load dynamic messages from dynamic message tables
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
    //sql_dynamic.push_str("\n  ORDER BY d.mmsi, d.time");

    let sql = format!(
        "WITH dynamic_{{}} AS (\n{}\n),\nstatic_{{}} AS ({}),\n{}{} \nORDER BY 1,2",
        sql_dynamic, sql_static, sql_coarsetype, sql_leftjoin
    );

    // parse client request into query parameters
    let qry = parse_request(req).expect("parsing request params");
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
    let stmt = pg.prepare(&sql_union).unwrap();
    let mut tx = pg.transaction().unwrap();
    let portal = tx
        .bind(
            &stmt,
            &[&qry.start, &qry.end, &area.x0, &area.x1, &area.y0, &area.y1],
        )
        .unwrap();

    GeneratorIteratorAdapter::new(track_generator(portal, tx))
}

fn compress_string_zlib(s: String) -> Result<Vec<u8>, std::io::Error> {
    //let mut e = ZlibEncoder::new(Vec::new(), Compression::Fast);
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(s.as_bytes())?;
    //websocket.write_message(Message::Binary(e.finish()?))?;
    let compressed = e.finish()?;
    println!(
        "compressed length: {} {:?}",
        compressed.len(),
        &compressed[..25]
    );

    use flate2::write::GzDecoder;
    //use std::io::Read;
    let mut d = GzDecoder::new(Vec::new());
    d.write_all(&compressed).unwrap();
    let decomp = d.finish()?;
    println!(
        "decompressed: {} {}",
        decomp.len(),
        String::from_utf8_lossy(&decomp)
    );

    //Ok(s.as_bytes().to_vec())
    Ok(compressed)
}

/// rounds float values in a track vector to 5 decimal places.
/// this is used to help with JSON message compression
fn round_floats(
    tracks: GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()>>,
    precision: f64,
) -> GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()>> {
    let multiplier = (1.0 / precision).round();
    GeneratorIteratorAdapter::new(move || {
        for mut track in tracks {
            for column_data in track.vectors.values_mut() {
                if column_data.is_empty() {
                    #[cfg(debug_assertions)]
                    eprintln!("warning: empty column in track vector data");
                    break;
                }
                match &column_data[0] {
                    TrackData::F(_f) => {
                        let rounded: Vec<TrackData> = column_data
                            .iter()
                            .map(|f| TrackData::F((f.as_float() * multiplier).round() / multiplier))
                            .collect::<Vec<TrackData>>();

                        *column_data = rounded;
                    }

                    _other => {
                        continue;
                    }
                }
            }
            yield track;
        }
    })
}

fn compress_geometry_vectors(
    tracks: GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()>>,
    precision: f64,
) -> GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()>> {
    GeneratorIteratorAdapter::new(move || {
        for track in tracks {
            let coords: Vec<Coord> = zip!(
                track.vectors.get("longitude").unwrap(),
                track.vectors.get("latitude").unwrap()
            )
            .map(|(xx, yy)| Coord {
                x: xx.clone().as_float(),
                y: yy.clone().as_float(),
            })
            .collect();
            let count_orig = coords.len();

            let mut mask: Vec<bool> = Vec::new();
            let mut idx_deque =
                VecDeque::from_iter(LineString(coords).simplify_vw_idx(&precision).into_iter());
            for i in 0..count_orig {
                if i == idx_deque[0] {
                    mask.push(true);
                    idx_deque.pop_front().unwrap();
                } else {
                    mask.push(false);
                }
            }
            assert!(mask.len() == count_orig);

            let mut newtrack = Track::new();
            newtrack.meta = track.meta.clone();
            for col in track.vectors.keys() {
                let f: Vec<TrackData> =
                    std::iter::zip(track.vectors.get(col).unwrap(), mask.clone())
                        .filter(|(_v, m)| *m)
                        .map(|(v, _m)| v.clone())
                        .collect();
                newtrack.vectors.insert(col.to_string(), f).unwrap();
            }

            yield newtrack
        }
    })
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
        tracks: GeneratorIteratorAdapter<impl Generator<Yield = Track, Return = ()>>,
        websocket: &mut tungstenite::WebSocket<TcpStream>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut count = 0;
        for track in tracks {
            let response = json!(Response {
                msgtype: "track_vector".to_string(),
                x: track.vectors.get("longitude").unwrap(),
                y: track.vectors.get("latitude").unwrap(),
                t: track.vectors.get("time").unwrap(),
                meta: track.meta,
            });
            websocket.write_message(Message::Binary(response.to_string().into()))?;
            //websocket.write_message(Message::Binary(compress_string_zlib( response.to_string(),)?))?;
            count += 1;
        }

        // websocket.write_pending()?; // flush write buffer

        let status_response = format!(
            "{{\"msgtype\":\"done\", \"status\":\"Done. Count: {}\"}}",
            count
        );
        websocket.write_message(Message::Binary(status_response.into()))?;
        //websocket.write_message(Message::Binary(compress_string_zlib(status_response)?))?;
        Ok(())
    }

    // timeouts and TLS are handled by gateway
    downstream.set_read_timeout(None)?;
    downstream.set_write_timeout(None)?;

    // accept websocket connection from incoming TCP connection
    let remote_hostname = downstream.peer_addr()?;
    let mut websocket = accept(downstream).expect("accepting websocket connection from client");

    // loop will await client requests and respond accordingly
    loop {
        println!("awaiting message from {} ...", remote_hostname);

        // parse client request
        let request_data = websocket.read_message()?;
        match request_data {
            Message::Ping(_) => {
                websocket.write_message(Message::Pong(vec![0u8]))?;
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

        // handle response depending on request type
        let response_result = match req.msgtype.as_str() {
            "track_vectors" => {
                let tracks = query_dynamic_tables(req, pg);
                let compressed = compress_geometry_vectors(tracks, 0.0001);
                let rounded = round_floats(compressed, 0.0001);
                write_response(rounded, &mut websocket)?;
                Ok(())
            }

            "track_vectors_extra" => {
                let tracks = query_merged_tables(req, pg);
                let compressed = compress_geometry_vectors(tracks, 0.0001);
                let rounded = round_floats(compressed, 0.0001);
                write_response(rounded, &mut websocket)?;
                Ok(())
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
                websocket.write_message(Message::Binary(response.to_string().into()))?;
                Ok(())
            }

            "meta" => {
                let (start, end) = query_validrange(pg)?;
                println!("sending vessel metadata...");
                let vinfo = query_metadata(pg, start, end);
                for obj in vinfo {
                    websocket.write_message(Message::Binary(obj.to_string().into()))?;
                }
                Ok(())
            }

            // Draw geojson polygons on client
            "zones" => {
                for zone in default_zones() {
                    websocket.write_message(Message::binary(zone.to_string()))?;
                }
                websocket.write_message(Message::Binary(
                    "{\"msgtype\":\"doneZones\"}".as_bytes().to_vec(),
                ))?;
                Ok(())
            }

            _unknown_query_type => Err("unknown query type"),
        };

        if let Err(e) = response_result {
            return Err(e.into());
        }
    }
}
