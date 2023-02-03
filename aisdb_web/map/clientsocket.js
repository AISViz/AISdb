/** @module clientsocket */
import { database_hostname, database_port, disable_ssl } from './constants.js';

window.statusmsg = null;
let newHeatmapFeatures = null;
let newPolygonFeature = null;
let newTrackFeature = null;
let searchbtn = null;
let resetSearchState = null;
let setSearchRange = null;


let doneLoadingRange = false;
let doneLoadingZones = false;

/** await until socket has returned timerange data */
async function waitForTimerange() {
  while (doneLoadingRange === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 10);
    });
  }
}

/** await until socket has returned zone polygons data */
async function waitForZones() {
  while (doneLoadingZones === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 10);
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

let process_response = null;

async function initialize_db_socket() {
  let socketHost = null;
  if (disable_ssl !== null && disable_ssl !== undefined) {
    console.log('CAUTION: connecting to websocket over unencrypted connection!');
    socketHost = `ws://${database_hostname}:${database_port}`;
  } else {
    /** @constant {string} socketHost socket host address */
    socketHost = `wss://${database_hostname}/ws`;
  }
  /** @constant {WebSocket} socket database websocket */
  let socket = new WebSocket(socketHost);

  /** closes connection to the server before exiting browser window
   * @callback window_onbeforeunload
   *
   */
  window.onbefureunload = async function() {
    // socket.onclose = function() {};
    await socket.close();
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
    // import { newHeatmapFeatures, newPolygonFeature, newTrackFeature } from './map';
    /** await until socket has returned timerange data */
    while (socket.onopen === undefined || process_response === null || newPolygonFeature === null || newTrackFeature === null || newHeatmapFeatures === null) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 10);
      });
    }

    let txt = await event.data.text();
    let response = JSON.parse(txt);
    if (response.msgtype === 'track_vector') {
      let processed = convert_utf8_js(process_response({
        rawdata:convert_js_utf8(response)
      }));
      // console.log(JSON.stringify(response['meta']['vesseltype_generic']));
      await newTrackFeature(processed, response.meta);
      await socket.send(JSON.stringify({ type: 'ack' }));
    } else if (response.msgtype === 'zone') {
      await socket.send(JSON.stringify({ type: 'ack' }));
      let processed = convert_utf8_js(process_response({
        rawdata:convert_js_utf8(response)
      }));
      processed.type = 'Polygon';
      processed.coordinates = [ processed.coordinates ];
      await newPolygonFeature(processed, response.meta);
    } else if (response.type === 'heatmap') {
      await newHeatmapFeatures(response.xy);
      await socket.send(JSON.stringify({ type: 'ack' }));
    } else if (response.type === 'done') {
      document.getElementById('status-div').textContent = response.status;
      window.statusmsg = response.status;
      searchbtn.textContent = 'Search';
      await resetSearchState();
    } else if (response.type === 'doneZones') {
      doneLoadingZones = true;
    } else if (response.type === 'validrange'){
      doneLoadingRange = true;
      setSearchRange(response.start, response.end);
    } else {
      let msg = 'Unknown response from server';
      document.getElementById('status-div').textContent = msg;
      window.statusmsg = msg;
    }
  };

  /** socket open event.
   * establishes connection with server and requests valid time ranges in database
   * @callback socket_onclose
   * @function
   * @param {Object} event onopen event
   */
  socket.onopen = async function() {
    // let msg = `Established connection to ${socketHost}`;
    let [
      { default: init, process_response: _process_response },
      { default: parseUrl },
      { searchbtn: _searchbtn,
        resetSearchState: _resetSearchState,
        setSearchRange: _setSearchRange,
      },
    ] = await Promise.all([
      import('./pkg/client'),
      import('./url'),
      import('./selectform'),
    ]);
    process_response = _process_response;
    searchbtn = _searchbtn;
    resetSearchState = _resetSearchState;
    setSearchRange = _setSearchRange;
    await init();

    let {
      newHeatmapFeatures: _newHeatmapFeatures,
      newPolygonFeature: _newPolygonFeature,
      newTrackFeature: _newTrackFeature
    } = await import('./map');
    newHeatmapFeatures = _newHeatmapFeatures;
    newPolygonFeature = _newPolygonFeature;
    newTrackFeature = _newTrackFeature;

    // first get valid DB query range from server
    await socket.send(JSON.stringify({ type: 'validrange' }));

    // wait for default search start/end values to be initialized
    await waitForTimerange();

    // override start/end values from GET request vars
    await parseUrl();
  };

  return socket;
}


export {
  initialize_db_socket,
  resetLoadingZones,
  // socket,
  waitForTimerange,
  waitForZones,
};
