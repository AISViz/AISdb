/** @module url */

/** @constant {URLSearchParams} urlParams parses GET request */
const urlParams = new URLSearchParams(window.location.search);


/** checks if n is numeric
 * @param {String} n string to be checked
 * @returns {boolean}
 */
function isNumeric(n) {
  if (isNaN(n) === false && n !== null) {
    return true;
  }
  return false;
}

/** set map display parameters via GET request. example:
 * http://localhost:3000/?ecoregions=1&x=-65&y=59.75&z=4&start=2021-01-01&end=2021-01-02&xmin=-95.7&xmax=-39.5&ymin=34.4&ymax=74.2
 */
async function parseUrl() {
  let { mapview } = await import('./map');

  if (isNumeric(urlParams.get('x')) && isNumeric(urlParams.get('y'))) {
    let { fromLonLat } = await import('ol/proj');
    let lon = parseFloat(urlParams.get('x'));
    let lat = parseFloat(urlParams.get('y'));
    mapview.setCenter(fromLonLat([ lon, lat, ]));
  }

  if (isNaN(urlParams.get('z')) === false && urlParams.get('z') !== null) {
    let zoom = parseFloat(urlParams.get('z'));
    mapview.setZoom(zoom);
  }

  if (Date.parse(urlParams.get('start')) > 0 &&
    Date.parse(urlParams.get('end')) > 0) {
    let { setSearchValue } = await import('./selectform');
    // document.getElementById('time-select-start').value = urlParams.get('start');
    // document.getElementById('time-select-end').value = urlParams.get('end');
    setSearchValue(urlParams.get('start'), urlParams.get('end'));
  }

  if (isNumeric(urlParams.get('xmin')) &&
    isNumeric(urlParams.get('xmax')) &&
    isNumeric(urlParams.get('ymin')) &&
    isNumeric(urlParams.get('ymax'))) {
    let xmin = parseFloat(urlParams.get('xmin'));
    let xmax = parseFloat(urlParams.get('xmax'));
    let ymin = parseFloat(urlParams.get('ymin'));
    let ymax = parseFloat(urlParams.get('ymax'));
    window.searcharea = {
      minX: xmin,
      maxX: xmax,
      minY: ymin,
      maxY: ymax,
    };
  }

  if (urlParams.get('ecoregions') !== undefined &&
    urlParams.get('ecoregions') !== null) {
    let {
      resetLoadingZones, socket, waitForZones
    } = await import('./clientsocket.js');
    await resetLoadingZones();
    await socket.send(JSON.stringify({ type: 'zones' }));
    await waitForZones();
  }

  if (urlParams.get('search') !== null) {
    let { searchbtn } = await import('./selectform');
    await searchbtn.click();

    if (urlParams.get('screenshot') !== null) {
      // import { screenshot } from './render';
      let { screenshot } = await import('./render');

      await screenshot();
    }
  }
  if (urlParams.get('debugopts') !== null) {
    console.log('enabling heatmap testing... (DEBUGOPTS=1)');
    window.heatmaptest = true;
  }
}

export default parseUrl;
