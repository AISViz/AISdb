/** @module selectform */
import flatpickr from 'flatpickr';
import 'flatpickr/dist/flatpickr.css';

import { socket, waitForTimerange } from './clientsocket';
import {
  addInteraction,
  clearFeatures,
  dragBox,
  draw,
  drawSource,
  lineSource,
  map,
  polySource,
  setSearchAreaFromSelected,
} from './map';

import {
  hiddenStyle,
  polyStyle,
  vesselStyles,
  vessellabels,
  vesseltypes,
} from './palette';


/** @constant {element} statusdiv status message div element */
const statusdiv = document.getElementById('status-div');
/** @constant {element} selectbtn select area button */
const selectbtn = document.getElementById('selectbtn');
/** @constant {element} timeselectstart time start date input */
const timeselectstart = document.getElementById('time-select-start');
/** @constant {element} timeselectend time end date input */
const timeselectend = document.getElementById('time-select-end');
/** @constant {element} vesseltypeselect filter by vessel type interaction */
const vesseltypeselect = document.getElementById('vesseltype-select');
/** @constant {element} searchbtn start new database search button */
const searchbtn = document.getElementById('searchbtn');
/** @constant {element} clearbtn map window reset button */
const clearbtn = document.getElementById('clearbtn');
/** @constant {element} selectmenu area selection popup menu element */
const selectmenu = document.getElementById('select-menu');
/** @constant {element} vesselmenu vessel type selection popup menu element */
const vesselmenu = document.getElementById('vesseltype-menu');


window.timeselectstart = timeselectstart;
const timeselectstart_fp = flatpickr(timeselectstart, {
  onChange: function(selectedDates, dateStr, instance) {
    timeselectstart.value = dateStr;
  }
});
const timeselectend_fp = flatpickr(timeselectend, {
  onChange: function(selectedDates, dateStr, instance) {
    timeselectend.value = dateStr;
  }
});

/** searchstate true if not currently performing a search
 * @see resetSearchState
 * @type {boolean}
 */
let searchstate = true;


/** reset the search state
 * @see searchstate
 */
async function resetSearchState() {
  searchstate = true;
}


/** awaits until searchstate is true
 * @see searchstate
 */
async function waitForSearchState() {
  while (searchstate === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 250);
    });
  }
}


/** cancel button action */
async function cancelSearch() {
  searchbtn.disabled = true;
  searchbtn.textContent = 'Search';
  statusdiv.textContent = 'Cancelling...';
  window.statusmsg = statusdiv.textContent;
  await socket.send(JSON.stringify({ type: 'stop' }));
  await waitForSearchState();
}


/** initiate new database query from server via socket
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
async function newSearch(start, end) {
  searchstate = false;
  statusdiv.textContent = 'Searching...';
  searchbtn.textContent = 'Cancel';
  window.statusmsg = statusdiv.textContent;
  let type = 'track_vectors';
  if (window.heatmaptest === true) {
    type = 'heatmap';
    console.log('debugging heatmap...');
  }
  await socket.send(JSON.stringify({
    type: type,
    start: start,
    end: end,
    area: window.searcharea,
  }));
  window.searcharea = null;
  await waitForSearchState();
}


/** select button click action
 * @callback selectbtn_onclick
 * @function
 */
selectbtn.onclick = function() {
  selectmenu.classList.toggle('show');
};


/** select menu options click actions
 * @callback selectmenu_childNodes_onclick
 * @function
 */
selectmenu.childNodes.forEach((opt) => {
  opt.onclick = async function() {
    selectmenu.classList.toggle('show');
    if (opt.dataset.value === 'ecoregions' &&
      polySource.getFeatures().length === 0) {
      map.removeInteraction(draw);
      map.removeInteraction(dragBox);
      drawSource.clear();
      await socket.send(JSON.stringify({ type: 'zones' }));
    } else if (opt.dataset.value === 'selectbox') {
      polySource.clear();
      map.removeInteraction(draw);
      map.removeInteraction(dragBox);
      drawSource.clear();
      addInteraction();
    }
  };
});


/** search button click action
 * @callback searchbtn_onclick
 * @function
 */
searchbtn.onclick = async function() {
  let start = timeselectstart.value;
  let end = timeselectend.value;

  await waitForTimerange();

  // validate input and create database request
  if (searchstate === false) {
    // if already searching, send STOP
    await cancelSearch();
  } else if (window.searcharea === null) {
    // validate area input
    statusdiv.textContent = 'Error: No area selected';
    window.statusmsg = statusdiv.textContent;
  } else if (start === '' || end === '') {
    // validate time input
    statusdiv.textContent = 'Error: No time selected';
    window.statusmsg = statusdiv.textContent;
  } else if (start >= end) {
    // validate time input
    statusdiv.textContent = 'Error: Start must occur before end';
    window.statusmsg = statusdiv.textContent;
  } else if (start < timeselectstart_fp.config.minDate) {
    // validate time input
    statusdiv.textContent = `Error: No data before ${timeselectstart.min}`;
    window.statusmsg = statusdiv.textContent;
  } else if (end > timeselectend_fp.config.maxDate) {
    // validate time input
    statusdiv.textContent = `Error: No data after ${timeselectend.max}`;
    window.statusmsg = statusdiv.textContent;
  } else if (Math.floor((new Date(end) - new Date(start)) / (1000 * 60 * 60 * 24)) > 31) {
    // validate time input
    statusdiv.textContent = 'Error: select a time range of one month or less';
    window.statusmsg = statusdiv.textContent;
  } else if (searchstate === true) {
    // create database request if everything is OK
    await newSearch(start, end);
  }
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
};


/** reset button click action
 * @callback clearbtn_onclick
 * @function
 */
clearbtn.onclick = async function() {
  selectbtn.textContent = 'Select Area';
  await setSearchAreaFromSelected();
  window.statusmsg = '';
  statusdiv.textContent = '';
  if (searchstate === false) {
    await cancelSearch();
  }
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
  clearFeatures();
};


/** set date input min/max values upon connection to server.
 * a timerange request is made on connection, and this function will be
 * called
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
async function setSearchRange(start, end) {
  timeselectstart_fp.set('minDate', start);
  timeselectend_fp.set('minDate', start);
  timeselectstart_fp.set('maxDate', end);
  timeselectend_fp.set('maxDate', end);
  let defaultStart = '2021-07-01';
  let defaultEnd = '2021-07-14';
  if (timeselectstart.value === '' &&
    timeselectend.value === '' &&
    start < defaultStart &&
    end > defaultEnd) {
    timeselectstart_fp.set('defaultDate', defaultStart);
    timeselectstart_fp.jumpToDate(defaultStart, true);
    timeselectstart.value = defaultStart;
    timeselectend_fp.set('defaultDate', defaultEnd);
    timeselectend_fp.jumpToDate(defaultEnd, true);
    timeselectend.value = defaultEnd;
  }
}


/** set start/end date input default values. used by module:url for setting
 * query time via GET request.
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
async function setSearchValue(start, end) {
  /*
  timeselectstart.value = start;
  timeselectend.value = end;
  */
  timeselectstart_fp.set('defaultDate', start);
  timeselectstart_fp.jumpToDate(start, true);
  timeselectend_fp.set('defaultDate', end);
  timeselectend_fp.jumpToDate(end, true);
}


/** selectedType
 * @type {String}
 */
let selectedType = 'All';


/** set feature style according to vessel type and currently displayed types
 * @param {ol.feature.Feature} ft track feature to be styled according to
 * current selectedType
 * @see selectedType
 */
function set_track_style(ft) {
  if (selectedType === 'All') {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else if (ft.get('meta').vesseltype_generic.includes(selectedType)) {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else {
    ft.setStyle(hiddenStyle);
  }
}


/** update all track features in lineSource according to current selectedType
 * @see selectedType
 */
function update_vesseltype_styles() {
  /* vessel types selector action */
  if (selectedType === 'All') {
    for (let ft of lineSource.getFeatures()) {
      ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
    }
  } else {
    for (let ft of lineSource.getFeatures()) {
      if (ft.get('meta').vesseltype_generic.includes(selectedType)) {
        ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
      } else {
        ft.setStyle(hiddenStyle);
      }
    }
  }
}


/** add a new item to the vesseltype selection popup menu
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
  let opt = document.createElement('a');
  let colordot = `<div class="colordot" style="color: rgb(${vesseltypes[label]});">${symbol}</div>`;
  opt.className = 'hiddenmenu-item';
  opt.innerHTML = `<div>${label}</div>&ensp;${colordot}`;
  opt.dataset.value = value;
  opt.onclick = function() {
    selectedType = opt.dataset.value;
    update_vesseltype_styles();
    vesseltypeselect.innerHTML = `Vessel Type ${colordot}`;
    vesselmenu.classList.toggle('show');
  };
  vesselmenu.appendChild(opt);
}
createVesselMenuItem('All', 'All', '⋀');
for (let label of vessellabels) {
  createVesselMenuItem(label, label);
}
createVesselMenuItem('Unknown', 'None', '○');
vesseltypeselect.onclick = function() {
  vesselmenu.classList.toggle('show');
};


// const downloadbtn = document.getElementById('downloadbtn');
// downloadbtn.style.display = 'none';

export {
  clearbtn,
  searchbtn,
  resetSearchState,
  setSearchRange,
  setSearchValue,
  set_track_style,
  update_vesseltype_styles,
  waitForSearchState,
};
