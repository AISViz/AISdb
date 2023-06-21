// https://rustwasm.github.io/wasm-bindgen/examples/websockets.html

use std::collections::HashMap;
use std::io::Read;
//use std::io::Write;

//use flate2::write::GzDecoder;
use flate2::read::GzDecoder;
use geo_types::{Coord, LineString};
//use geo::algorithm::simplifyvw::SimplifyVW;
use geojson::{Geometry, Value};
use serde::{Deserialize, Serialize};

extern crate wasm_bindgen;
use js_sys::JsString;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;
//use web_sys::{console, ErrorEvent, MessageEvent, WebSocket};

extern crate serde_wasm_bindgen;
//use serde_wasm_bindgen::from_value;

#[cfg(debug_assertions)]
use std::panic;

#[cfg(debug_assertions)]
extern crate console_error_panic_hook;

#[derive(Serialize, Deserialize)]
//#[wasm_bindgen]
pub struct Geojs {
    pub rawdata: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct GzipMsg {
    pub payload: String,
}

/// Track vector data from postgres queries may contain these types
#[derive(Debug, Serialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum TrackData {
    I(i32),
    F(f64),
    S(String),
}

/// server response to track vectors request
#[derive(Serialize)]
struct Response<'a> {
    pub msgtype: String,
    pub x: &'a Vec<TrackData>,
    pub y: &'a Vec<TrackData>,
    pub t: &'a Vec<TrackData>,
    pub meta: HashMap<String, String>,
}

/// server response to valid date range as a UNIX timestamp
#[derive(Serialize, Deserialize)]
struct DaterangeResponse {
    pub msgtype: String,
    pub start: i32,
    pub end: i32,
}

/*
#[derive(Serialize, Deserialize)]
pub enum TrackData {
S(String),
I(i32),
F(f64),
}
*/

#[derive(Serialize, Deserialize)]
pub struct GeometryVector {
    pub msgtype: String,
    pub x: Vec<f32>,
    pub y: Vec<f32>,
    pub t: Vec<f32>,
    pub meta: HashMap<String, String>,
    //pub meta: HashMap<String, TrackData>,
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[allow(unused_macros)]
macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

macro_rules! zip {
    ($x: expr) => ($x);
    ($x: expr, $($y: expr), +) => (
        $x.iter().zip(
            zip!($($y), +))
        )
}

/*
fn from_u16(from: &mut [u16]) -> &[u8] {
    if cfg!(target_endian = "little") {
        for byte in from.iter_mut() {
            *byte = byte.to_be();
        }
    }

    let len = from.len().checked_mul(2).unwrap();
    let ptr: *const u8 = from.as_ptr().cast();
    unsafe { std::slice::from_raw_parts(ptr, len) }
}
*/

//#[wasm_bindgen]
pub fn unzip(gzipped: JsString) {
    //use std::panic;
    //panic::set_hook(Box::new(console_error_panic_hook::hook));

    assert!(JsString::is_valid_utf16(&gzipped));

    //let gzipped = gzipped.as_string().unwrap();
    //let encoded: Vec<u16> = gzipped.encode_utf16().collect();

    //let s = String::from_utf8_lossy(&gzipped);
    //let s = &gzipped;
    //console_log!("wasm - received {} ({}): {}", s.len(), gzipped.len(), s,);
    //gzipped.to_vec()

    //let mut d = GzDecoder::new(Vec::new());

    //d.write_all(&gzipped).expect("writing to decoder");
    //d.finish().expect("finishing decoder")

    // JsString attempt
    //let mut i: Vec<u16> = gzipped.iter().collect();
    //let i = from_u16(&mut i);
    //let s: String;
    //unsafe {
    //    s = String::from_utf8_unchecked(i.to_vec());
    //}
    //console_log!("wasm - received {}: {}", s.len(), s,);

    /*
       let mut bts: Vec<u8> = Vec::new();
       for c in gzipped.iter() {
       bts.push(c.try_into().unwrap());
       }
    //bts.splice(0.., [31u8, 139u8, 8u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
    console_log!(
    "wasm - received {}: {:?}\n{}",
    bts.len(),
    &bts,
    String::from_utf8_lossy(&bts)
    );
    //unsafe { JsValue::from(String::from_utf8_unchecked(bts)) }
    */

    let trim = gzipped.trim();
    assert!(trim.is_valid_utf16());

    //d.write_all(&gzipped.as_string().unwrap().as_bytes()) .expect("writing to decoder");
    //d.finish().expect("finishing decoder");
    //let c = trim.as_string().unwrap();

    let c = gzipped.as_string().unwrap();
    let mut gz = GzDecoder::new(c.as_bytes());

    //let s = String::new();
    let mut v: Vec<u8> = Vec::new();
    //gz.read_to_string(&mut s).unwrap();
    gz.read_to_end(&mut v).unwrap();

    //JsValue::from_utf8(v)

    //s.as_bytes().to_vec()
}

#[wasm_bindgen]
pub fn process_response(txt: JsValue) -> JsValue {
    #[cfg(debug_assertions)]
    console_log!("info: debug hooks enabled");
    #[cfg(debug_assertions)]
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let raw: Geojs = serde_wasm_bindgen::from_value(txt).expect("deserializing js");
    let response_chars = std::str::from_utf8(&raw.rawdata).unwrap();
    //console_log!("{}", &response_chars);
    let geom: GeometryVector = serde_json::from_str(response_chars).unwrap();
    let coords = zip!(&geom.x, &geom.y)
        .map(|(xx, yy)| Coord { x: *xx, y: *yy })
        .collect();
    //let line = LineString(coords).simplifyvw(&0.001);
    let line = LineString(coords);
    let simplified_coords = line
        .into_iter()
        .map(|p| vec![p.x as f64, p.y as f64])
        .collect::<Vec<Vec<f64>>>();
    let linegeojs = Geometry::new(Value::LineString(simplified_coords));
    let rawresponse: Vec<u8> = linegeojs.to_string().chars().map(|x| x as u8).collect();
    let payload = Geojs {
        rawdata: rawresponse,
    };

    serde_wasm_bindgen::to_value(&payload.rawdata).unwrap()
}
