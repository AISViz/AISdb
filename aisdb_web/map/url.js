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
  let { mapview } = await import('./map.js');

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
    let { setSearchValue } = await import('./selectform.js');
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
      resetLoadingZones, waitForZones
    } = await import('./clientsocket.js');
    await resetLoadingZones();
    await window.socket.send(JSON.stringify({ type: 'zones' }));
    await waitForZones();
  }

  if (urlParams.get('search') !== null) {
    let { searchbtn } = await import('./selectform.js');
    await searchbtn.click();

    if (urlParams.get('screenshot') !== null) {
      // import { screenshot } from './render';
      let { screenshot } = await import('./render');

      await screenshot();
    }
  }

  if (urlParams.get('24h') !== null && urlParams.get('24h') !== undefined) {
    if (window.searcharea === null || window.searcharea === undefined) {
      window.searcharea = {
        minX: -180.0,
        maxX: 180.0,
        minY: -90,
        maxY: 90,
      };
    }

    let now = new Date();
    let yesterday = new Date(new Date().getTime() - 2 * 24 * 60 * 60 * 1000);
    let now_str = `${now.getUTCFullYear()}-${now.getUTCMonth() + 1 }-${now.getUTCDate()}`;
    let yesterday_str = `${yesterday.getUTCFullYear()}-${yesterday.getUTCMonth() + 1 }-${yesterday.getUTCDate()}`;

    let { setSearchValue } = await import('./selectform');
    setSearchValue(yesterday_str, now_str);

    let { searchbtn } = await import ('./selectform.js');
    searchbtn.click();
  }


  if (urlParams.get('debugopts') !== null) {
    console.log('enabling heatmap testing... (DEBUGOPTS=1)');
    window.heatmaptest = true;
  }
}

export default parseUrl;
