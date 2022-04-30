/** @module clientsocket */
import { newPolygonFeature, newTrackFeature } from './map';
import { process_response } from './pkg/client';
import { searchbtn, resetSearchState, setSearchRange } from './selectform';
import parseUrl from './url';


/** socket server hostname as read from $VITE_AISDBHOST env variable
 * @constant {string} hostname
 */
let hostname = import.meta.env.VITE_AISDBHOST;
if (hostname === undefined) {
  hostname = 'localhost';
}

/** socket server port as read from $VITE_AISDBPORT env variable
 * @constant {string} hostname
 */
let port = import.meta.env.VITE_AISDBPORT;
if (port === undefined) {
  port = '9924';
}


/** @constant {string} socketHost socket host address */
const socketHost = `wss://${hostname}/ws`;
/** @constant {WebSocket} socket database websocket */
let socket = new WebSocket(socketHost);


let doneLoadingRange = false;
let doneLoadingZones = false;

/** await until socket has returned timerange data */
async function waitForTimerange() {
  while (doneLoadingRange === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 250);
    });
  }
}

/** await until socket has returned zone polygons data */
async function waitForZones() {
  while (doneLoadingZones === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 250);
    });
  }
}

/** reset the zone polygons await state */
async function resetLoadingZones() {
  doneLoadingZones = false;
}

let utf8encode = new TextEncoder();
/** Convert object to UTF8 integer array. Used for passing values to WebAssembly
 * scripts
 * @param {Object} obj arbitrary JSON
 * @returns {Array} UTF8 integer Array
 */
function convert_js_utf8(obj) {
  return Array.from(utf8encode.encode(JSON.stringify(obj)));
}

let utf8decode = new TextDecoder();
/** Convert UTF8 integer array to Object. Used for receiving values from
 * WebAssembly scripts
 * @param {Array} arr UTF8 integer Array
 * @returns {Object} JSON from UTF8 integer array
 */
function convert_utf8_js(arr) {
  return JSON.parse(utf8decode.decode(new Uint8Array(arr)));
}

window.statusmsg = null;
/** socket open event.
 * establishes connection with server and requests valid time ranges in database
 * @callback socket_onclose
 * @function
 * @param {Object} event onopen event
 */
socket.onopen = async function(event) {
  let msg = `Established connection to ${socketHost}`;
  console.log(msg);
  await socket.send(JSON.stringify({ type: 'validrange' }));
  await waitForTimerange();
  await parseUrl();
};

/** socket close event.
 * ends connection with server and displays a status message
 * @callback socket_onclose
 * @function
 * @param {Object} event onclose event
 */
socket.onclose = function(event) {
  let msg = null;
  if (event.wasClean) {
    msg = 'Closed connection with server';
  } else {
    msg = `Unexpected error occurred, please refresh the page [${event.code}]`;
  }
  console.log(msg);
  document.getElementById('status-div').textContent = msg;
  window.statusmsg = msg;
};

/** socket error event.
 * displays an error in the status message
 * @callback socket_onerror
 * @function
 * @param {Object} event onerror event
 */
socket.onerror = function(event) {
  let msg = `An unexpected error occurred [${event.code}]`;
  console.log(msg);
  document.getElementById('status-div').textContent = msg;
  window.statusmsg = msg;
  socket.close();
};

/** socket message event.
 * handles messages from server according to response type
 * @callback socket_onmessage
 * @function
 * @param {Object} event onmessage event
 */
socket.onmessage = async function(event) {
  let response = JSON.parse(event.data);
  if (response.msgtype === 'track_vector') {
    let processed = convert_utf8_js(process_response({
      rawdata:convert_js_utf8(response)
    }));
    // console.log(JSON.stringify(response['meta']['vesseltype_generic']));
    newTrackFeature(processed, response.meta);
    await socket.send(JSON.stringify({ type: 'ack' }));
  } else if (response.msgtype === 'zone') {
    let processed = convert_utf8_js(process_response({
      rawdata:convert_js_utf8(response)
    }));
    processed.type = 'Polygon';
    processed.coordinates = [ processed.coordinates ];
    newPolygonFeature(processed, response.meta);
    await socket.send(JSON.stringify({ type: 'ack' }));
  } else if (response.type === 'done') {
    document.getElementById('status-div').textContent = response.status;
    window.statusmsg = response.status;
    searchbtn.disabled = false;
    searchbtn.textContent = 'Search';
    await resetSearchState();
    window.searcharea = null;
    // if (searchstate === false) {
    //  searchbtn.click();
    // }
  } else if (response.type === 'doneZones') {
    doneLoadingZones = true;
  } else if (response.type === 'validrange'){
    await setSearchRange(response.start, response.end);
    doneLoadingRange = true;
  } else {
    let msg = 'Unknown response from server';
    document.getElementById('status-div').textContent = msg;
    window.statusmsg = msg;
  }
};

/** closes connection to the server before exiting browser window
 * @callback window_onbeforeunload
 *
 */
window.onbefureunload = function() {
  socket.onclose = function() {};
  socket.close();
};


// window.show_zones = function() {
//  socket.send(JSON.stringify({ type: 'zones' }));
// };

export { socket, waitForTimerange, waitForZones, resetLoadingZones };
