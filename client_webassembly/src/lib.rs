use std::collections::HashMap;

use geo_types::{Coord, LineString};
use geojson::{Geometry, Value};
use serde::{Deserialize, Serialize};

extern crate wasm_bindgen;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::JsValue;

extern crate serde_wasm_bindgen;

#[cfg(debug_assertions)]
use std::panic;

#[cfg(debug_assertions)]
extern crate console_error_panic_hook;

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct GeometryVector {
    pub msgtype: String,
    pub x: Vec<f32>,
    pub y: Vec<f32>,
    pub t: Vec<f32>,
    pub meta: HashMap<String, String>,
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

#[wasm_bindgen]
pub fn process_response(txt: JsValue) -> JsValue {
    #[cfg(debug_assertions)]
    console_log!("info: debug hooks enabled");
    #[cfg(debug_assertions)]
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let raw: Geojs = serde_wasm_bindgen::from_value(txt).expect("deserializing js");
    let response_chars = std::str::from_utf8(&raw.rawdata).unwrap();
    let geom: GeometryVector = serde_json::from_str(response_chars).unwrap();
    let coords = zip!(&geom.x, &geom.y)
        .map(|(xx, yy)| Coord { x: *xx, y: *yy })
        .collect();
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
