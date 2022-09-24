// https://rustwasm.github.io/book/reference/code-size.html
// https://rustwasm.github.io/wasm-bindgen/examples/websockets.html
use geo_types::{Coordinate, LineString};
//use geo::algorithm::simplifyvw::SimplifyVW;
use geojson::{Geometry, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str;
use wasm_bindgen::prelude::*;
//use wasm_bindgen::JsCast;
//use web_sys::{console, ErrorEvent, MessageEvent, WebSocket};

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
pub struct GeometryVector {
    pub msgtype: String,
    pub x: Vec<f32>,
    pub y: Vec<f32>,
    pub t: Vec<f32>,
    pub meta: HashMap<String, String>,
}

/*
#[link(wasm_import_module = "../map/map")]
extern "C" {
//fn handle_response();
fn newGeoVectorLayer(geojs: JsValue);
}
*/

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
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let raw: Geojs = JsValue::into_serde(&txt).expect("this");
    let response_chars = std::str::from_utf8(&raw.rawdata).unwrap();
    let geom: GeometryVector = serde_json::from_str(response_chars).unwrap();
    let coords = zip!(&geom.x, &geom.y)
        .map(|(xx, yy)| Coordinate { x: *xx, y: *yy })
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

    JsValue::from_serde(&payload.rawdata).unwrap()
}

/*
   fn handle_msg_abuf(abuf: js_sys::ArrayBuffer, cloned_ws: WebSocket) -> Result<(), JsValue> {
   console_log!("message event, received arraybuffer: {:?}", abuf);
   let array = js_sys::Uint8Array::new(&abuf);
   let len = array.byte_length() as usize;
   console_log!("Arraybuffer received {} bytes: {:?}", len, array.to_vec());
// here you can for example use Serde Deserialize decode the message
// for demo purposes we switch back to Blob-type and send off another binary message
cloned_ws.set_binary_type(web_sys::BinaryType::Blob);
match cloned_ws.send_with_u8_array(&vec![5, 6, 7, 8]) {
Ok(_) => console_log!("binary message successfully sent"),
Err(err) => console_log!("error sending message: {:?}", err),
}
Ok(())
}
*/

/*
#[wasm_bindgen]
pub fn handle_msg_blob(blob: web_sys::Blob, cloned_ws: WebSocket) -> Result<(), JsValue> {
//panic::set_hook(Box::new(console_error_panic_hook::hook));
console_log!("message event, received blob: {:?}", blob);
// better alternative to juggling with FileReader is to use https://crates.io/crates/gloo-file
let fr = web_sys::FileReader::new().unwrap();
let fr_c = fr.clone();
// create onLoadEnd callback
let onloadend_cb = Closure::wrap(Box::new(move |_e: web_sys::ProgressEvent| {
let array = js_sys::Uint8Array::new(&fr_c.result().unwrap()).to_vec();
//let len = array.byte_length() as usize;
//console_log!("Blob received {} bytes: {:?}", len, array.to_vec());
// here you can for example use the received image/png data
let payload = std::str::from_utf8(&array).unwrap();
//console_log!("stringdata: {:?}", payload);

//let rawdata = JsValue::from_str(payload);
//console_log!("jsvalue rawdata: {:?}", rawdata);

//let res: Geojs = serde_json::from_str(rawdata).unwrap();
//console_log!("res.rawdata: {:?}", res.rawdata);
let geom: GeometryVector = serde_json::from_str(payload).unwrap();
console_log!("{:?}", geom.meta);
/*
match cloned_ws.send_with_str("{\"dtype\":\"ack\"}") {
Ok(_) => console_log!("requesting more track vectors..."),
Err(err) => console_log!("error sending message: {:?}", err),
}
*/
match cloned_ws.send_with_str("{\"dtype\":\"done\"}") {
    Ok(_) => console_log!("stopping..."),
    Err(err) => console_log!("error sending message: {:?}", err),
}
    let coords = zip!(&geom.x, &geom.y)
.map(|(xx, yy)| Coordinate { x: *xx, y: *yy })
    .collect();
    let line = LineString(coords).simplifyvw_preserve(&0.01);
    console_log!("simplified line: {:?}", line);
    //
    let simplified_coords = line
    .into_iter()
.map(|p| vec![p.x as f64, p.y as f64])
    .collect::<Vec<Vec<f64>>>();
    console_log!("simplified line arr: {:?}", simplified_coords);
    let linegeojs = Geometry::new(Value::LineString(simplified_coords));
    console_log!("geojson: {}", linegeojs.to_string());
    let geochars: Vec<char> = linegeojs.to_string().chars().collect::<Vec<char>>();
    let geobytes: Vec<u8> = geochars.into_iter().map(|x| x as u8).collect();
    let payload = Geojs { rawdata: geobytes };
/*
   unsafe {
   newGeoVectorLayer(JsValue::from_serde(&payload).unwrap());
   }
   */
}) as Box<dyn FnMut(web_sys::ProgressEvent)>);
fr.set_onloadend(Some(onloadend_cb.as_ref().unchecked_ref()));
fr.read_as_array_buffer(&blob).expect("blob not readable");
onloadend_cb.forget();
//let t = blob.text();
//let s = std::str::from_utf8(t.to_string()).unwrap();
//let geom: GeometryVector = serde_json::from_str(&s).unwrap();

Ok(())
    }
*/

/*
#[wasm_bindgen]
pub fn handle_msg_txt(txt: JsValue) -> JsValue {
//console_log!("message event, received Text: {:?}", txt);
let mut raw: Geojs = JsValue::into_serde(&txt).expect("this");
console_log!("raw: {:?}", raw.rawdata);
raw.rawdata = vec![1, 5, 6, 3];
console_log!("raw transformed: {:?}", raw.rawdata);
//let s = std::str::from_utf8(&raw.rawdata).unwrap();
//console_log!("string: {:?}", s);
//let geom: GeometryVector = serde_json::from_str(&s).unwrap();
//JsValue::from_serde(&geom).unwrap()

//

JsValue::from_serde(&raw.rawdata).unwrap()
}
*/

/*
//#[wasm_bindgen(start)]
pub fn webclient() -> Result<(), JsValue> {
panic::set_hook(Box::new(console_error_panic_hook::hook));

console::log_1(&"Contacting host...".into());
let ws = WebSocket::new("ws://localhost:9924")?;

console::log_1(&"Connected to host".into());
let cloned_ws = ws.clone();

let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
handle_msg_abuf(abuf, ws.clone()).unwrap();
} else if let Ok(blob) = e.data().dyn_into::<web_sys::Blob>() {
handle_msg_blob(blob, ws.clone()).unwrap();
//} else if let txt = e.data() {
} else if let Ok(txt) = e.data().dyn_into::<JsValue>() {
console_log!("txt data rcv {:?}", txt);
//unsafe {
//    handle_response();
//}
handle_msg_txt(txt);
} else {
console_log!("message event, received Unknown: {:#?}", e.data());
}
}) as Box<dyn FnMut(MessageEvent)>);

// set message event handler on WebSocket
cloned_ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
// forget the callback to keep it alive
onmessage_callback.forget();

let onerror_callback = Closure::wrap(Box::new(move |e: ErrorEvent| {
console_log!("error event: {:?}", e);
}) as Box<dyn FnMut(ErrorEvent)>);
cloned_ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
onerror_callback.forget();

let cloned_ws1 = cloned_ws.clone();
let onopen_callback = Closure::wrap(Box::new(move |_| {
/*
console_log!("socket opened");
match cloned_ws.send_with_str("ping") {
Ok(_) => console_log!("message successfully sent"),
Err(err) => console_log!("error sending message: {:?}", err),
}
// send off binary message
match cloned_ws.send_with_u8_array(&vec![0, 1, 2, 3]) {
Ok(_) => console_log!("binary message successfully sent"),
Err(err) => console_log!("error sending message: {:?}", err),
}
*/
match cloned_ws1
.send_with_str("{\"dtype\": \"track_vectors_week\", \"date\": \"2016-05-01\"}")
{
    Ok(_) => console_log!("requesting track vectors..."),
    Err(err) => console_log!("error sending message: {:?}", err),
}
}) as Box<dyn FnMut(JsValue)>);
cloned_ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
onopen_callback.forget();

Ok(())
    }
*/

/*
#[wasm_bindgen]
pub fn prepare_json(res: &JsValue) -> JsValue {
//pub fn prepare_json(res: &str) -> JsValue {
//panic::set_hook(Box::new(console_error_panic_hook::hook));
//let geom: Geometry = res.into_serde().unwrap();
console_log!("RUST RCV {:?}", res);
let geom = Geometry {
opts: vec![1, 2, 3],
payload: vec![4, 5, 6],
};

//JsValue::from_serde(&geom).unwrap()
JsValue::from_serde(&res).unwrap()
}
*/
