import { newPolygonFeature, newTrackFeature } from './map';
import { process_response } from '../pkg/client';
import { setSearchRange } from './selectform';

let hostname = import.meta.env.VITE_AISDBHOST;
if (hostname === undefined) {
  hostname = 'localhost';
}
let port = import.meta.env.VITE_AISDBPORT;
if (port === undefined) {
  port = '9924';
}

let utf8encode = new TextEncoder();
let utf8decode = new TextDecoder();

// const socketHost = `ws://${hostname}:${port}`
const socketHost = `wss://${hostname}/ws`;
let socket = new WebSocket(socketHost);

function js_utf8(obj) {
  return Array.from(utf8encode.encode(JSON.stringify(obj)));
}
function utf8_js(obj) {
  return JSON.parse(utf8decode.decode(new Uint8Array(obj)));
}

window.statusmsg = null;
socket.onopen = function(event) {
  let msg = `Established connection to ${socketHost}`;
  console.log(msg);
  socket.send(JSON.stringify({ type: 'validrange' }));
};
socket.onclose = function(event) {
  let msg = null;
  if (event.wasClean) {
    msg = 'Closed connection with server';
  } else {
    msg = `Connection to server died unexpectedly [${event.code}]`;
  }
  console.log(msg);
  document.getElementById('status-div').textContent = msg;
  window.statusmsg = msg;
};
socket.onerror = function(error) {
  let msg = 'An unexpected error occurred';
  console.log(msg);
  document.getElementById('status-div').textContent = msg;
  window.statusmsg = msg;
  socket.close();
};
socket.onmessage = async function(event) {
  let response = JSON.parse(event.data);
  if (response.type === 'WKBHex') {
    for (const geom in response.geometries) {
      newPolygonFeature(
        [ response.geometries[geom].geometry ],
        response.geometries[geom].meta
      );
    }
  } else if (response.msgtype === 'track_vector') {
    let processed = utf8_js(process_response({ rawdata:js_utf8(response) }));
    // console.log(JSON.stringify(response['meta']['vesseltype_generic']));
    newTrackFeature(processed, response.meta);
    await socket.send(JSON.stringify({ type: 'ack' }));
  } else if (response.msgtype === 'zone') {
    // let processed = utf8_js(process_response({'rawdata':js_utf8(response)}));
    // console.log(JSON.stringify(response['meta']['vesseltype_generic']));
    // newTrackFeature(processed, response['meta']);
    // await socket.send(JSON.stringify({'type': 'ack'}));
    let processed = utf8_js(process_response({ rawdata:js_utf8(response) }));
    processed.type = 'Polygon';
    processed.coordinates = [ processed.coordinates ];
    newPolygonFeature(processed, response.meta);
    // await socket.send(JSON.stringify({'type': 'ack'}));
  } else if (response.type === 'done') {
    document.getElementById('status-div').textContent = response.status;
    document.getElementById('searchbtn').disabled = false;
    window.statusmsg = response.status;
    // searchbtn.disabled = false;
  } else if (response.type === 'validrange'){
    setSearchRange(response.start, response.end);
  } else {
    let msg = 'Unknown response from server';
    document.getElementById('status-div').textContent = msg;
    window.statusmsg = msg;
  }
};
window.onbefureunload = function() {
  socket.onclose = function() {};
  socket.close();
};


// window.show_zones = function() {
//  socket.send(JSON.stringify({ type: 'zones' }));
// };

export default socket;
