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

/** Await until socket has returned timerange data */
async function waitForTimerange() {
  while (doneLoadingRange === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 10);
    });
  }
}

/** Await until socket has returned zone polygons data */
async function waitForZones() {
  while (doneLoadingZones === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 10);
    });
  }
}

/** Reset the zone polygons await state */
async function resetLoadingZones() {
  doneLoadingZones = false;
}

const utf8encode = new TextEncoder();
/** Convert object to UTF8 integer array. Used for passing values to WebAssembly
 * scripts
 * @param {Object} object arbitrary JSON
 * @returns {Array} UTF8 integer Array
 */
function convert_js_utf8(object) {
  return Array.from(utf8encode.encode(JSON.stringify(object)));
}

const utf8decode = new TextDecoder();
/** Convert UTF8 integer array to Object. Used for receiving values from
 * WebAssembly scripts
 * @param {Array} array UTF8 integer Array
 * @returns {Object} JSON from UTF8 integer array
 */
function convert_utf8_js(array) {
  return JSON.parse(utf8decode.decode(new Uint8Array(array)));
}

let process_response = null;

/** Start the database websocket connection */
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
  const socket = new WebSocket(socketHost);

  /** Closes connection to the server before exiting browser window
   * @callback window_onbeforeunload
   *
   */
  window.onbefureunload = async function () {
    // Socket.onclose = function() {};
    await socket.close();
  };

  /** Socket close event.
   * ends connection with server and displays a status message
   * @callback socket_onclose
   * @function
   * @param {Object} event onclose event
   */
  socket.addEventListener('close', (event) => {
    let message = null;
    message = event.wasClean ? 'Closed connection with server' : `Unexpected error occurred, please refresh the page [${event.code}]`;

    console.log(message);
    document.querySelector('#status-div').textContent = message;
    window.statusmsg = message;
  });

  /** Socket error event.
   * displays an error in the status message
   * @callback socket_onerror
   * @function
   * @param {Object} event onerror event
   */
  socket.onerror = function (event) {
    const message = `An unexpected error occurred [${event.code}]`;
    console.log(message);
    document.querySelector('#status-div').textContent = message;
    window.statusmsg = message;
    socket.close();
  };

  /** Socket message event.
   * handles messages from server according to response type
   * @callback socket_onmessage
   * @function
   * @param {Object} event onmessage event
   */
  socket.onmessage = async function (event) {
    // Import { newHeatmapFeatures, newPolygonFeature, newTrackFeature } from './map';
    /** await until socket has returned timerange data */
    while (socket.onopen === undefined ||
      process_response === null ||
      newPolygonFeature === null ||
      newTrackFeature === null ||
      newHeatmapFeatures === null) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 10);
      });
    }

    const txt = await event.data.text();
    const response = JSON.parse(txt);
    if (response.msgtype === 'track_vector') {
      const processed = convert_utf8_js(process_response({
        rawdata: convert_js_utf8(response),
      }));
      // Console.log(JSON.stringify(response['meta']['vesseltype_generic']));
      await newTrackFeature(processed, response.meta);
      await socket.send(JSON.stringify({ type: 'ack' }));
    } else if (response.msgtype === 'zone') {
      await socket.send(JSON.stringify({ type: 'ack' }));
      const processed = convert_utf8_js(process_response({
        rawdata: convert_js_utf8(response),
      }));
      processed.type = 'Polygon';
      processed.coordinates = [ processed.coordinates ];
      await newPolygonFeature(processed, response.meta);
    } else {
      switch (response.type) {
      case 'heatmap': {
        await newHeatmapFeatures(response.xy);
        await socket.send(JSON.stringify({ type: 'ack' }));

        break;
      }

      case 'done': {
        document.querySelector('#status-div').textContent = response.status;
        window.statusmsg = response.status;
        searchbtn.textContent = 'Search';
        await resetSearchState();

        break;
      }

      case 'doneZones': {
        doneLoadingZones = true;

        break;
      }

      case 'validrange': {
        doneLoadingRange = true;
        setSearchRange(response.start, response.end);

        break;
      }

      default: {
        const message = 'Unknown response from server';
        document.querySelector('#status-div').textContent = message;
        window.statusmsg = message;
      }
      }
    }
  };

  /** Socket open event.
   * establishes connection with server and requests valid time ranges in database
   * @callback socket_onclose
   * @function
   * @param {Object} event onopen event
   */
  socket.addEventListener('open', async () => {
    // Let msg = `Established connection to ${socketHost}`;
    const [
      { default: init, process_response: _process_response },
      { default: parseUrl },
      { searchbtn: _searchbtn,
        resetSearchState: _resetSearchState,
        setSearchRange: _setSearchRange,
      },
    ] = await Promise.all([
      import('./pkg/client.js'),
      import('./url.js'),
      import('./selectform.js'),
    ]);
    process_response = _process_response;
    searchbtn = _searchbtn;
    resetSearchState = _resetSearchState;
    setSearchRange = _setSearchRange;
    await init();

    const {
      newHeatmapFeatures: _newHeatmapFeatures,
      newPolygonFeature: _newPolygonFeature,
      newTrackFeature: _newTrackFeature,
    } = await import('./map.js');
    newHeatmapFeatures = _newHeatmapFeatures;
    newPolygonFeature = _newPolygonFeature;
    newTrackFeature = _newTrackFeature;

    // First get valid DB query range from server
    await socket.send(JSON.stringify({ type: 'validrange' }));

    // Wait for default search start/end values to be initialized
    await waitForTimerange();

    // Override start/end values from GET request vars
    await parseUrl();
  });

  return socket;
}

export {
  initialize_db_socket,
  resetLoadingZones,
  // Socket,
  waitForTimerange,
  waitForZones,
};
