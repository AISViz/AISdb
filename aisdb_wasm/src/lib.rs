// https://rustwasm.github.io/book/reference/code-size.html
// https://rustwasm.github.io/wasm-bindgen/examples/websockets.html
use js_sys::JsString;
use serde::{Deserialize, Serialize};
use std::panic;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{console, ErrorEvent, MessageEvent, WebSocket};
extern crate console_error_panic_hook;

#[derive(Serialize, Deserialize)]
pub struct GeometryJSON {
    pub payload: JsString,
    pub opts: JsString,
}

#[link(wasm_import_module = "../map/map")]
extern "C" {
    fn handle_response(res: JsString);
}

#[wasm_bindgen]
extern "C" {
    pub fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet(name: &str) {
    alert(&format!("Hello, {}!", name));
}

macro_rules! console_log {
    ($($t:tt)*) => (console::log_1(&format_args!($($t)*).to_string().into()))
}

#[wasm_bindgen(start)]
pub fn webclient() -> Result<(), JsValue> {
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    console::log_1(&"Contacting host...".into());
    let ws = WebSocket::new("ws://localhost:9924")?;

    console::log_1(&"Connected to host".into());
    let cloned_ws = ws.clone();

    let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
        if let Ok(blob) = e.data().dyn_into::<web_sys::Blob>() {
            console_log!("message event, received blob: {:?}", blob);
            // better alternative to juggling with FileReader is to use https://crates.io/crates/gloo-file
            let fr = web_sys::FileReader::new().unwrap();
            let fr_c = fr.clone();
            // create onLoadEnd callback
            let onloadend_cb = Closure::wrap(Box::new(move |_e: web_sys::ProgressEvent| {
                let array = js_sys::Uint8Array::new(&fr_c.result().unwrap());
                let len = array.byte_length() as usize;
                console_log!("Blob received {}bytes: {:?}", len, array.to_vec());
                // here you can for example use the received image/png data
            })
                as Box<dyn FnMut(web_sys::ProgressEvent)>);
            fr.set_onloadend(Some(onloadend_cb.as_ref().unchecked_ref()));
            fr.read_as_array_buffer(&blob).expect("blob not readable");
            onloadend_cb.forget();
        } else if let Ok(txt) = e.data().dyn_into::<js_sys::JsString>() {
            console_log!("message event, received Text: {:?}", txt);
            unsafe {
                handle_response(txt);
            }
        } else {
            console_log!("message event, received Unknown: {:?}", e.data());
        }
    }) as Box<dyn FnMut(MessageEvent)>);

    // set message event handler on WebSocket
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
    // forget the callback to keep it alive
    onmessage_callback.forget();

    let onerror_callback = Closure::wrap(Box::new(move |e: ErrorEvent| {
        console_log!("error event: {:?}", e);
    }) as Box<dyn FnMut(ErrorEvent)>);
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
    onerror_callback.forget();

    let cloned_ws = ws.clone();
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
        match cloned_ws.send_with_str("{\"type\": \"zones\"}") {
            Ok(_) => console_log!("message successfully sent"),
            Err(err) => console_log!("error sending message: {:?}", err),
        }
    }) as Box<dyn FnMut(JsValue)>);
    ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
    onopen_callback.forget();

    Ok(())
}
