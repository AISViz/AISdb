/**@module clientsocket */

import {
  resetSearchState,
  searchbtn,
  setValidSearchRange,
} from './selectform.js';

import {
  lineSource,
  newHeatmapFeatures,
  newPolygonFeature,
  newTrackFeature,
  polySource,
} from './map.js';

import {
  database_hostname,
  database_port,
  debug,
  disable_ssl_db
} from './constants.js';


import init, { process_response } from './pkg/client.js?init';


window.statusmsg = null;
let doneLoadingRange = false;
let doneLoadingZones = false;
//let doneLoadingMetadata = false;
let doneLoadingSocket = false;

/**
  vesselInfo contains the metadata for each vessel sent by the server:
  e.g. vesselInfo[205535690]:
        Object { mmsi: 205535690, msgtype: "vesselinfo", meta_string: "MMSI: 205535690<br>" }
*/
let vesselInfo = {};
window.vesselInfo = vesselInfo;


/**@constant {WebSocket} socket database websocket */
let socket = null; //new WebSocket(socketHost);

let socketHost = null;
if (disable_ssl_db !== null && disable_ssl_db !== undefined) {
  console.log('CAUTION: connecting to websocket over unencrypted connection!');
  socketHost = `ws://${database_hostname}:${database_port}`;
} else {
  /**@constant {string} socketHost socket host address */
  socketHost = `wss://${database_hostname}/ws`;
}


//wait for content to load
const timeout = (prom, time) => {
  return Promise.race([ prom, new Promise((_r, rej) => {
    return setTimeout(rej, time);
  }) ]);
};

/**await until socket has returned timerange data */
async function waitForTimerange() {
  async function _waittimerange() {
    while (doneLoadingRange === false) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 50);
      });
    }
  }
  await timeout(_waittimerange(), 10000).catch((err) => {
    console.error('timed out waiting for timerange from server');
  });
}

/**Await until socket has returned zone polygons data */
async function waitForZones() {
  async function _waitzones() {
    while (doneLoadingZones === false) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 50);
      });
    }
  }
  await timeout(_waitzones(), 10000).catch((err) => {
    console.error('timed out waiting for zones from server');
  });
}

/**Await until socket has returned zone polygons data */
/*
async function waitForMetadata() {
  while (doneLoadingMetadata === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 50);
    });
  }
}
*/

async function waitForSocket() {
  async function _waitsocket() {
    while (doneLoadingSocket === false) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 50);
      });
    }
  }
  await timeout(_waitsocket(), 10000).catch((err) => {
    console.error('timed out waiting for DB socket');
  });
  /*
  timeout(async () => {
    while (doneLoadingSocket === false) {
      await new Promise((resolve) => {
        return setTimeout(resolve, 50);
      });
    }
  }, 5000).catch((err) => {
    console.error('timed out waiting for DB socket');
  });
  */
}

/**Reset the zone polygons await state */
async function resetLoadingZones() {
  doneLoadingZones = false;
}

const utf8encode = new TextEncoder();
/**Convert object to UTF8 integer array. Used for passing values to WebAssembly
 * scripts
 * @param {Object} object arbitrary JSON
 * @returns {Array} UTF8 integer Array
 */
function convert_js_utf8(object) {
  return Uint8Array.from(utf8encode.encode(JSON.stringify(object)));
}

const utf8decode = new TextDecoder();
/**Convert UTF8 integer array to Object. Used for receiving values from
 * WebAssembly scripts
 * @param {Array} array UTF8 integer Array
 * @returns {Object} JSON from UTF8 integer array
 */
function convert_utf8_js(array) {
  return JSON.parse(utf8decode.decode(new Uint8Array(array)));
}

function handle_zone(response) {
  const processed = convert_utf8_js(process_response({
    rawdata: convert_js_utf8(response),
  }));
  processed.type = 'Polygon';
  processed.coordinates = [ processed.coordinates ];

  newPolygonFeature(processed, response.meta);
}

/**Socket message event.
   * handles messages from server according to response type
   * @callback socket_onmessage
   * @function
   * @param {Object} event onmessage event
   */
async function handle_server_response(event) {
  let response = null;
  let txt = null;
  try {
    txt = await event.data.text();
  } catch (e) {
    console.error('could not get event data!\n', e);
    return;
  }
  response = JSON.parse(txt);


  if (!('msgtype' in response)) {
    console.error('unknown response type:', response);
  }

  switch (response.msgtype) {
  case 'track_vector': {
    const res_utf8 = convert_js_utf8(response);
    const processed_utf8 = process_response({ rawdata: res_utf8 });
    const processed = convert_utf8_js(processed_utf8);

    /*
    const processed = convert_utf8_js(process_response({
      rawdata: convert_js_utf8(response),
    }));
    */
    newTrackFeature(processed, response.meta.mmsi);
    break;
  }

  case 'vesselinfo': {
    //await handle_vesselinfo(response);
    //await socket.send('ack');
    console.error('vesselinfo is handled by socket in vessel_metadata.ts');
    break;
  }

  case 'zone': {
    handle_zone(response);
    break;
  }

  case 'heatmap': {
    newHeatmapFeatures(response.xy);
    await socket.send(JSON.stringify({ msgtype: 'ack' }));

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

  case 'doneMetadata': {
    //doneLoadingMetadata = true;
    console.error('vesselinfo is handled by socket in db.js');

    break;
  }

  case 'validrange': {
    doneLoadingRange = true;
    setValidSearchRange(response.start * 1000, response.end * 1000);

    break;
  }

  default: {
    const message = 'Unknown response from server';
    console.log(response);
    document.querySelector('#status-div').textContent = message;
    window.statusmsg = message;
  }
  }
}


/**Start the database websocket connection */
async function initialize_db_socket() {
  await init();

  socket = new WebSocket(socketHost);

  /**Closes connection to the server before exiting browser window
   * @callback window_onbeforeunload
   *
   */
  window.onbefureunload = function () {
    socket.addEventListener('close', () => {});
    socket.close();
  };

  /**Socket close event.
   * ends connection with server and displays a status message
   * @callback socket_onclose
   * @function
   * @param {Object} event onclose event
   */
  socket.addEventListener('close', async (event) => {
    const message = 'Error: session was terminated. Retrying...';
    console.log(message);
    document.querySelector('#status-div').textContent = message;
    window.statusmsg = message;
    await new Promise((r) => {
      return setTimeout(r, Math.random() * 5000 + 5000);
    });
    initialize_db_socket();
  });

  /**Socket error event.
   * displays an error in the status message
   * @callback socket_onerror
   * @function
   * @param {Object} event onerror event
   */
  socket.onerror = async (event) => {
    socket.close();
  };

  socket.onmessage = handle_server_response;

  /**Socket open event.
   * establishes connection with server and requests valid time ranges in database
   * @callback socket_onclose
   * @function
   * @param {Object} event onopen event
   */
  socket.addEventListener('open', async () => {
    //clear previous status
    document.querySelector('#status-div').textContent = '';
    window.statusmsg = '';

    //reset any existing zones
    polySource.clear();
    await resetLoadingZones();


    //query valid time ranges and zone polygons
    await timeout(Promise.all([
      socket.send(JSON.stringify({ msgtype: 'validrange' })),
      socket.send(JSON.stringify({ msgtype: 'zones' })),
      waitForTimerange(),
      waitForZones(),
    ]), 15000).catch(() => {
      return console.log('timed out loading data from server!');
    });

    //await waitForDB();
    //const vesselObjStore = vesselInfoDB.transaction('VesselInfoDB', 'readwrite').objectStore('VesselInfoDB');

    /*
    await Promise.all([
      socket.send(JSON.stringify({ msgtype: 'meta' })),
      waitForMetadata(),
    ]);
    */
    doneLoadingSocket = true;
  });

  if (debug !== null && debug !== undefined) {
    console.log('done db socket initialization');
  }

  return;
}

export {
  initialize_db_socket,
  resetLoadingZones,
  socket as db_socket,
  socketHost as db_socket_host,
  timeout,
  vesselInfo,
  //waitForMetadata,
  waitForSocket,
  waitForTimerange,
  waitForZones,
};
