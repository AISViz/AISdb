import { newPolygonFeature, newTrackFeature } from "./map";
import { vesseltypes }  from "./palette";
import { process_response } from "../pkg/client";
import { setSearchRange } from "./selectform";

let hostname = import.meta.env.VITE_AISDBHOST;
if (hostname == undefined) {
  hostname = 'localhost';
}
let port = import.meta.env.VITE_AISDBPORT;
if (port == undefined) {
  port = '9924';
}

let utf8encode = new TextEncoder();
let utf8decode = new TextDecoder();

const socketHost = `ws://${hostname}:${port}`
let socket = new WebSocket(socketHost);

function js_utf8(obj) {
  return Array.from(utf8encode.encode(JSON.stringify(obj)));
}
function utf8_js(obj) {
  return JSON.parse(utf8decode.decode(new Uint8Array(obj)));
}

window.statusmsg = null;
socket.onopen = function(event) {
  console.log(`Established connection to ${socketHost}\nCaution: connection is unencrypted!`);
  //socket.send(JSON.stringify({'type': 'zones'}));
  socket.send(JSON.stringify({'type': 'validrange'}));
}
socket.onclose = function(event) {
  if (event.wasClean) {
    let msg = `Closed connection with server`;
    console.log(msg);
    document.getElementById('status-div').textContent = msg;
    window.statusmsg = msg;
  } else {
    let msg = `Connection to server died unexpectedly`;
    console.log(msg);
    document.getElementById('status-div').textContent = msg;
    window.statusmsg = msg;
  }
}
socket.onerror = function(error) {
  let msg = `An unexpected error occurred`;
  console.log(msg);
  document.getElementById('status-div').textContent = msg;
  window.statusmsg = msg;
  socket.close();
}
socket.onmessage = async function(event) {
  let response = JSON.parse(event.data);
  if (response['type'] === 'WKBHex') {
    for (const geom in response['geometries']) {
      newPolygonFeature(
        [response['geometries'][geom]['geometry']],
        response['geometries'][geom]['meta']
      );
    }
  } else if (response['msgtype'] === 'track_vector') {
    /*
    for (const geom in response['geometries']) {
      newTopoVectorLayer(
        response['geometries'][geom]['topology'],
        response['geometries'][geom]['opts']
      );
    }
    */
    let processed = utf8_js(process_response({'rawdata':js_utf8(response)}));
    //console.log(JSON.stringify(response['meta']['vesseltype_generic']));
    newTrackFeature(processed, response['meta']);
    await socket.send(JSON.stringify({'type': 'ack'}));
    //await socket.send(JSON.stringify({'type': 'stop'}));
  }

  else if (response['type'] === 'done') {
    document.getElementById('status-div').textContent = response['status'];
    document.getElementById('searchbtn').disabled = false;
    window.statusmsg = response['status'];
    //searchbtn.disabled = false;
  } else if (response['type'] === 'validrange'){
    setSearchRange(response['start'], response['end']);
  } else {
    let msg = `Unknown response from server`;
    document.getElementById('status-div').textContent = msg;
    window.statusmsg = msg;
  }
}
window.onbefureunload = function() {
  socket.onclose = function() {} ;
  socket.close();
}

export default socket;
