/**@module url */

/**@constant {URLSearchParams} urlParams parses GET request */
const urlParameters = new URLSearchParams(window.location.search);

/**Checks if n is numeric
 * @param {String} n string to be checked
 * @returns {boolean}
 */
function isNumeric(n) {
  if (isNaN(n) === false && n !== null) {
    return true;
  }

  return false;
}

/**Set map display parameters via GET request. example:
 * http://localhost:3000/?zones=1&x=-65&y=59.75&z=4&start=2021-01-01&end=2021-01-02&xmin=-95.7&xmax=-39.5&ymin=34.4&ymax=74.2
 */
async function parseUrl() {
  const { mapview } = await import('./map.js');

  if (isNumeric(urlParameters.get('x')) && isNumeric(urlParameters.get('y'))) {
    const { fromLonLat } = await import('ol/proj');
    const lon = Number.parseFloat(urlParameters.get('x'));
    const lat = Number.parseFloat(urlParameters.get('y'));
    mapview.setCenter(fromLonLat([ lon, lat ]));
  }

  if (isNaN(urlParameters.get('z')) === false && urlParameters.get('z') !== null) {
    const zoom = Number.parseFloat(urlParameters.get('z'));
    mapview.setZoom(zoom);
  }

  if (Date.parse(urlParameters.get('start')) > 0 &&
    Date.parse(urlParameters.get('end')) > 0) {
    const { setSearchValue } = await import('./selectform.js');
    const t0 = new Date(urlParameters.get('start'));
    const t1 = new Date(urlParameters.get('end'));
    setSearchValue(t0.toISOString(), t1.toISOString());
  }

  if (isNumeric(urlParameters.get('xmin')) &&
    isNumeric(urlParameters.get('xmax')) &&
    isNumeric(urlParameters.get('ymin')) &&
    isNumeric(urlParameters.get('ymax'))) {
    const xmin = Number.parseFloat(urlParameters.get('xmin'));
    const xmax = Number.parseFloat(urlParameters.get('xmax'));
    const ymin = Number.parseFloat(urlParameters.get('ymin'));
    const ymax = Number.parseFloat(urlParameters.get('ymax'));
    window.searcharea = {
      x0: xmin,
      x1: xmax,
      y0: ymin,
      y1: ymax,
    };
  }

  //Load zone geometries from server
  //NOTE: this has become the default behaviour and added to socket.onopen event in clientsocket.js
  /*
  if (urlParameters.get('zones') !== undefined &&
    urlParameters.get('zones') !== null) {
    const {
      resetLoadingZones, waitForZones, waitForTimerange,
    } = await import('./clientsocket.js');
    await resetLoadingZones();
    await window.socket.send(JSON.stringify({ msgtype: 'zones' }));
    await waitForZones();
  }
  */

  if (urlParameters.get('python') !== undefined && urlParameters.get('python') !== null) {
    document.getElementById('formDiv').style.display = 'none';
    document.getElementById('mapDiv').style.height = '100%';
    const { waitForSocket, db_socket } = await import('./clientsocket.js');
    await waitForSocket();
    db_socket.addEventListener('close', async (event) => {
      window.close();
    });
    db_socket.addEventListener('error', async (event) => {
      window.close();
    });
  }

  if (urlParameters.get('search') !== null) {
    const { searchbtn } = await import('./selectform.js');
    await searchbtn.click();

    if (urlParameters.get('screenshot') !== null) {
      const { screenshot } = await import('./render.js');
      await screenshot();
    }
  }

  if (urlParameters.get('24h') !== null && urlParameters.get('24h') !== undefined) {
    const { setSearchValue, searchbtn } = await import('./selectform.js');

    if (window.searcharea === null || window.searcharea === undefined) {
      window.searcharea = {
        x0: -180,
        x1: 180,
        y0: -90,
        y1: 90,
      };
    }

    const daylength = 1 * 24 * 60 * 60 * 1000;
    //const offset = new Date(1970, 0, 0);
    const yesterday = new Date(Date.now() - daylength);
    const now = new Date(Date.now() - 1000 * 60 * 5); //latest results may have a 5 minute delay or longer if the message buffers are not filled
    //const now_string = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
    //const yesterday_string = `${yesterday.getUTCFullYear()}-${String(yesterday.getUTCMonth() + 1).padStart(2, '0')}-${String(yesterday.getUTCDate()).padStart(2, '0')}`;
    const now_string = now.toISOString();
    const yesterday_string = yesterday.toISOString();

    setSearchValue(yesterday_string, now_string);

    searchbtn.click();
  }

  if (urlParameters.get('debugopts') !== null) {
    console.log('enabling heatmap testing... (DEBUGOPTS=1)');
    window.heatmaptest = true;
  }
}

export default parseUrl;
