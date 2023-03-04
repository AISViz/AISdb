/** @module url */

/** @constant {URLSearchParams} urlParams parses GET request */
const urlParameters = new URLSearchParams(window.location.search);

/** Checks if n is numeric
 * @param {String} n string to be checked
 * @returns {boolean}
 */
function isNumeric(n) {
  if (isNaN(n) === false && n !== null) {
    return true;
  }

  return false;
}

/** Set map display parameters via GET request. example:
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
    // Document.getElementById('time-select-start').value = urlParams.get('start');
    // document.getElementById('time-select-end').value = urlParams.get('end');
    setSearchValue(urlParameters.get('start'), urlParameters.get('end'));
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
      minX: xmin,
      maxX: xmax,
      minY: ymin,
      maxY: ymax,
    };
  }

  if (urlParameters.get('zones') !== undefined &&
    urlParameters.get('zones') !== null) {
    const {
      resetLoadingZones, waitForZones,
    } = await import('./clientsocket.js');
    await resetLoadingZones();
    await window.socket.send(JSON.stringify({ type: 'zones' }));
    await waitForZones();
  }

  if (urlParameters.get('search') !== null) {
    const { searchbtn } = await import('./selectform.js');
    await searchbtn.click();

    if (urlParameters.get('screenshot') !== null) {
      // Import { screenshot } from './render';
      const { screenshot } = await import('./render.js');

      await screenshot();
    }
  }

  if (urlParameters.get('24h') !== null && urlParameters.get('24h') !== undefined) {
    if (window.searcharea === null || window.searcharea === undefined) {
      window.searcharea = {
        minX: -180,
        maxX: 180,
        minY: -90,
        maxY: 90,
      };
    }

    const now = new Date(Date.now() + 1 * 24 * 60 * 60 * 1000);
    const yesterday = new Date(Date.now() - 1 * 24 * 60 * 60 * 1000);
    const now_string = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
    const yesterday_string = `${yesterday.getUTCFullYear()}-${String(yesterday.getUTCMonth() + 1).padStart(2, '0')}-${String(yesterday.getUTCDate()).padStart(2, '0')}`;

    const { setSearchValue } = await import('./selectform.js');
    setSearchValue(yesterday_string, now_string);

    const { searchbtn } = await import('./selectform.js');
    searchbtn.click();
  }

  if (urlParameters.get('debugopts') !== null) {
    console.log('enabling heatmap testing... (DEBUGOPTS=1)');
    window.heatmaptest = true;
  }
}

export default parseUrl;
