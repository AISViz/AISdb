/**@module selectform */
import flatpickr from 'flatpickr';
import 'flatpickr/dist/flatpickr.css';

import { Fill, Stroke, Style } from 'ol/style';

import { debug, no_db_limit } from './constants.js';
import { db_socket, waitForTimerange, vesselInfo, waitForSocket } from './clientsocket.js';
import {
  dragBox,
  draw,
  drawSource,
  lineSource,
  setSearchAreaFromSelected,
  map,
} from './map.js';

import {
  hiddenStyle,
  vesselStyles,
  vessellabels,
  vesseltypes,
  zoom_line_width,
} from './palette.js';

/**@constant {element} searchbtn start new database search button */
const searchbtn = document.querySelector('#searchbtn');
/**@constant {element} clearbtn map window reset button */
const clearbtn = document.querySelector('#clearbtn');
/**@constant {element} vesselmenu vessel type selection popup menu element */
const vesselmenu = document.querySelector('#vesseltype-menu');
/**@constant {element} vesseltypeselect filter by vessel type interaction */
const vesseltypeselect = document.querySelector('#vesseltype-select');
/**@constant {element} statusdiv status message div element */
const statusdiv = document.querySelector('#status-div');
/**@constant {element} timeselectstart time start date input */
const timeselectstart = document.querySelector('#time-select-start');
/**@constant {element} timeselectend time end date input */
const timeselectend = document.querySelector('#time-select-end');

/*
Let map = null;
async function mapHook(hook) {
  map = hook;
}
*/

/**Searchstate true if not currently performing a search
 * @see resetSearchState
 * @type {boolean}
 */
let searchstate = true;

/**Reset the search state
 * @see searchstate
 */
async function resetSearchState() {
  searchstate = true;
}

/**Awaits until searchstate is true
 * @see searchstate
 */
async function waitForSearchState() {
  while (searchstate === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 10);
    });
  }
}

/**Cancel button action */
async function cancelSearch(db_socket) {
  searchbtn.textContent = 'Search';
  statusdiv.textContent = 'Cancelled search';
  window.statusmsg = statusdiv.textContent;
  document.body.style.cursor = 'initial';
  await resetSearchState();
  await db_socket.send(JSON.stringify({ type: 'stop' }));
}

/**Initiate new database query from server via socket
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
async function newSearch(start, end, db_socket) {
  if (db_socket === null) {
    console.error('async error: db_socket not initialized');
  }

  searchstate = false;
  statusdiv.textContent = 'Searching...';
  searchbtn.textContent = 'Cancel';
  window.statusmsg = statusdiv.textContent;
  let msgtype = 'track_vectors';
  if (window.heatmaptest === true) {
    msgtype = 'heatmap';
    console.log('debugging heatmap...');
  }

  const msg = JSON.stringify({
    msgtype: msgtype,
    start: start,
    end: end,
    area: window.searcharea,
  });
  console.log(`Sending database request to: ${db_socket.url}\n${msg}`);

  await db_socket.send(msg);
  window.searcharea = null;
}

/// ** @constant {element} selectmenu area selection popup menu element */
//const selectmenu = document.querySelector('#select-menu');

const timeselectstart_fp = flatpickr(timeselectstart, {
  onChange: function(selectedDates, dateString, instance) {
    timeselectstart.value = dateString;
  },
  altInput: true,
  altFormat: 'Z',
  enableTime: true,
  time_24hr: true,
  defaultDate: new Date(Date.now()).getTime() - 1000 * 60 * 60 * 24,
});
const timeselectend_fp = flatpickr(timeselectend, {
  onChange: function(selectedDates, dateString, instance) {
    timeselectend.value = dateString;
  },
  //dateFormat:
  altInput: true,
  altFormat: 'Z',
  enableTime: true,
  time_24hr: true,
  defaultDate: new Date(Date.now()).getTime() - 1000 * 60 * 5,
});

/**Set date input min/max values upon connection to server.
 * a timerange request is made on connection, and this function will be
 * called
 * @param {string} start start time as retrieved from date input
 * @param {string} end end time as retrieved from date input
 */
function setValidSearchRange(start, end) {
  timeselectstart_fp.set('minDate', new Date(start - 1000 * 60 * 60 * 24));
  timeselectend_fp.set('minDate', new Date(start));
  timeselectstart_fp.set('maxDate', new Date(end));
  timeselectend_fp.set('maxDate', new Date(end));
}

/**Set start/end date input default values. used by module:url for setting
 * query time via GET request.
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
function setSearchValue(start, end) {
  timeselectstart.value = start;
  timeselectend.value = end;
}

/**SelectedType
 * @type {String}
 */
let selectedType = 'All';

/**Set feature style according to vessel type and currently displayed types
 * @param {ol.feature.Feature} ft track feature to be styled according to
 * current selectedType
 * @see selectedType
 */
function set_track_style(ft) {
  const vinfo = vesselInfo[ft.getId()];
  if (vinfo === undefined) {
    //console.log('vinfo undefined for', ft.getId());
    ft.setStyle(vesselStyles.Unknown);
    return;
  }

  let label = null; //= vesselInfo[ft.getId()].vesseltype_generic;
  if (ft.getId() in vesselInfo && 'vesseltype_generic' in vinfo) {
    label = vinfo.vesseltype_generic;
  } else if (ft.getId() in vesselInfo && 'ship_type_txt' in vinfo) {
    label = vinfo.ship_type_txt;
  } else {
    label = 'Unknown';
  }

  if (selectedType === 'All' || label.includes(selectedType)) {
    //if the track contains 'color' key in metadata, use this color
    if (ft.getId() in vesselInfo && 'color' in vinfo) {
      ft.setStyle((feature, zoom) => {
        return new Style({
          fill: new Fill({
            color: vinfo.color,
          }),
          stroke: new Stroke({
            color: vinfo.color,
            width: zoom_line_width(zoom),
          }),

        });
      });
    //otherwise, default to a specific color for each type of vessel
    } else {
      ft.setStyle(vesselStyles[label]);
    }
  } else {
    ft.setStyle(hiddenStyle);
  }
}

/**Update all track features in lineSource according to current selectedType
 * @param {ol.source.Vector} lineSource target layer
 * @see selectedType
 */
function update_vesseltype_styles(lineSource) {
  /*Vessel types selector action */

  if (selectedType === 'All') {
    for (const ft of lineSource.getFeatures()) {
      const vinfo = vesselInfo[ft.getId()];
      let label = 'Unknown';
      if (vinfo === undefined) {
        //Pass

      } else if ('vesseltype_generic' in vinfo) {
        label = vinfo.vesseltype_generic;
      } else if ('ship_type_txt' in vinfo) {
        label = vinfo.ship_type_txt;
      }

      ft.setStyle(vesselStyles[label]);
    }
  } else {
    for (const ft of lineSource.getFeatures()) {
      const vinfo = vesselInfo[ft.getId()];
      let label = 'Unknown';
      if (vinfo === undefined) {} else if ('vesseltype_generic' in vinfo) {
        label = vinfo.vesseltype_generic;
      } else if ('ship_type_txt' in vinfo) {
        label = vinfo.ship_type_txt;
      }

      if (label.includes(selectedType)) {
        ft.setStyle(vesselStyles[label]);
      } else {
        ft.setStyle(hiddenStyle);
      }
    }
  }
}

/**Add a new item to the vesseltype selection popup menu
 * @param {String} label menu item text
 * @param {String} value menu item value - should be one of vessellabels
 * @param {String} symbol unicode symbol to display next to item label
 * @see module:palette:vessellabels
 * @see module:palette:vesselStyles
 * @see update_vesseltype_styles
 */
function createVesselMenuItem(label, value, symbol) {
  if (symbol === undefined) {
    symbol = '●';
  }

  const opt = document.createElement('div');
  const colordot = `<div class="colordot" style="color: rgb(${vesseltypes[label]});">${symbol}</div>`;
  opt.className = 'hiddenmenu-item';
  opt.innerHTML = `<div>${label}</div>&ensp;${colordot}`;
  opt.dataset.value = value;
  opt.addEventListener('click', () => {
    selectedType = opt.dataset.value;
    update_vesseltype_styles(lineSource);
    vesseltypeselect.innerHTML = `Vessel Type ${colordot}`;
    vesselmenu.classList.toggle('show');
  });

  vesselmenu.append(opt);
}

async function waitForMap() {
  while (map === null || map === undefined) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 25);
    });
  }
}

/**load elements for map selection form */
async function initialize_selectform() {
  const selectbtn = document.querySelector('#selectbtn');

  /**Select button click action
 * @callback selectbtn_onclick
 * @function
 */
  selectbtn.addEventListener('click', async () => {
    await waitForMap();
    //Selectmenu.classList.toggle('show');
    map.removeInteraction(draw);
    map.removeInteraction(dragBox);
    drawSource.clear();
    map.addInteraction(draw);
    map.addInteraction(dragBox);
    document.body.style.cursor = 'crosshair';
  });

  /**search button click action
   * @callback searchbtn_onclick
   * @function
   */
  searchbtn.addEventListener('click', async () => {
    let start = new Date(timeselectstart.value);
    let end = new Date(timeselectend.value);
    //const offset = new Date(Date.UTC(1970, 0, 0, 0, 0)); //milliseconds;
    //start = new Date(start.getTime() + offset.getTime() * -1);
    //end = new Date(end.getTime() + offset.getTime() * -1);
    //console.log(`offset: ${offset.toISOString()}\t start: ${start.toISOString()}\tend: ${end.toISOString()}`);

    await waitForTimerange();
    const daylength = 1000 * 60 * 60 * 24;
    const max_time = new Date(timeselectend_fp.config.maxDate.getTime());
    const min_time = new Date(timeselectstart_fp.config.minDate.getTime());

    //Validate time input
    if (start.getTime() < min_time.getTime()) {
      //statusdiv.textContent = `Warning: No data before ${min_time}\t got: ${new Date(start.getTime() + offset.getTime())}`;
      statusdiv.textContent = `Warning: No data before ${min_time.toISOString()}\t got: ${start.toISOString()}`;
      console.log(statusdiv.textContent);
      window.statusmsg = statusdiv.textContent;
      start = min_time;
      timeselectstart.value = min_time.toISOString();
    }
    if (end.getTime() > max_time.getTime()) {
      //statusdiv.textContent = `Warning: No data after ${max_time}\t got: ${new Date(end.getTime() + offset.getTime())}`;
      statusdiv.textContent = `Warning: No data before ${max_time.toISOString()}\t got: ${end.toISOString()}`;
      console.log(statusdiv.textContent);
      window.statusmsg = statusdiv.textContent;
      end = max_time;
      timeselectend.value = max_time.toISOString();
    }

    //Validate input and create database request
    if (searchstate === false) {
      //If already searching, send STOP
      await waitForSocket();
      await cancelSearch(db_socket);
    } else if (window.searcharea === null) {
      //Validate area input
      statusdiv.textContent = 'Error: No area selected';
      window.statusmsg = statusdiv.textContent;
    } else if (start === '' || end === '') {
      //Validate time input
      statusdiv.textContent = 'Error: No time selected';
      window.statusmsg = statusdiv.textContent;
    } else if (start.getTime() >= end.getTime()) {
      //Validate time input
      statusdiv.textContent = 'Error: Start must occur before end';
      window.statusmsg = statusdiv.textContent;
    } else if (Math.floor((end.getTime() - start.getTime()) / daylength) > 31 && (no_db_limit === undefined || no_db_limit === null)) {
      //Validate time input
      statusdiv.textContent = 'Error: select a time range of one month or less';
      window.statusmsg = statusdiv.textContent;
    } else if (searchstate === true) {
      //Create database request if everything is OK
      await waitForSocket();
      //console.log(`start: ${start.toISOString()}\tend: ${end.toISOString()}`);
      await newSearch(Math.round(start.getTime() / 1000), Math.round(end.getTime() / 1000), db_socket);
    }

    await waitForMap();
    map.removeInteraction(draw);
    map.removeInteraction(dragBox);
    document.body.style.cursor = 'initial';
  });

  /**Reset button click action
 * @callback clearbtn_onclick
 * @function
 */
  clearbtn.addEventListener('click', async () => {
    selectbtn.textContent = 'Select Area';
    await setSearchAreaFromSelected();
    window.statusmsg = '';
    statusdiv.textContent = '';
    if (searchstate === false) {
      await waitForSocket();
      await cancelSearch(db_socket);
    }

    await waitForMap();
    map.removeInteraction(draw);
    map.removeInteraction(dragBox);
    document.body.style.cursor = 'initial';
    drawSource.clear();
    lineSource.clear();
  });

  createVesselMenuItem('All', 'All', '⋀');
  for (const label of vessellabels) {
    createVesselMenuItem(label, label);
  }

  createVesselMenuItem('Unknown', 'None', '○');
  vesseltypeselect.addEventListener('click', () => {
    vesselmenu.classList.toggle('show');
  });

  if (debug !== null && debug !== undefined) {
    console.log('done select form initialization');
  }

  return selectbtn;
}

export {
  clearbtn,
  createVesselMenuItem,
  initialize_selectform,
  //MapHook,
  resetSearchState,
  searchbtn,
  setValidSearchRange,
  setSearchValue,
  set_track_style,
  update_vesseltype_styles,
  vesselmenu,
  vesseltypeselect,
  waitForSearchState,
};
