import {newWKBHexVectorLayer, newTopoVectorLayer} from "./map";

let hostname = import.meta.env.VITE_AISDBHOST;
if (hostname == undefined) {
  hostname = 'localhost';
}
let port = import.meta.env.VITE_AISDBPORT;
if (port == undefined) {
  port = '9924';
}

const socketHost = `ws://${hostname}:${port}`
let socket = new WebSocket(socketHost);

let errmsg = {
  0: 'Done',
  1: 'Stopped',
}

socket.onopen = function(event) {
  console.log(`Established connection to ${socketHost}\nCaution: connection is unencrypted!`);
  socket.send(JSON.stringify({'type': 'zones'}));
}
socket.onclose = function(event) {
  if (event.wasClean) {
    console.log(`[${event.code}] Closed connection with ${socketHost}`);
  } else {
    console.log(`[${event.code}] Connection to ${socketHost} died unexpectedly`);
  }
}
socket.onerror = function(error) {
  console.log(`[${error.code}] ${error.message}`);
  socket.close();
}
socket.onmessage = async function(event) {
  let response = JSON.parse(event.data);
  window.last = response;
  if (response['type'] === 'WKBHex') {
    for (const geom in response['geometries']) {
      newWKBHexVectorLayer(
        [response['geometries'][geom]['geometry']], 
        response['geometries'][geom]['opts']
      );
    }
  } else if (response['type'] === 'topology') {
    for (const geom in response['geometries']) {
      newTopoVectorLayer(
        response['geometries'][geom]['topology'], 
        response['geometries'][geom]['opts']
      );
    }
    await socket.send(JSON.stringify({'type': 'ack'}));
  } else if (response['type'] === 'done') {
    document.getElementById('status-div').textContent = errmsg[response['status']];
  }
}
window.onbefureunload = function() {
  socket.onclose = function() {} ;
  socket.close();
}

export default socket;
