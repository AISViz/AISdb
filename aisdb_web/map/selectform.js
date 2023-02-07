/** @module selectform */
import flatpickr from 'flatpickr';
import 'flatpickr/dist/flatpickr.css';
// Import 'tiny-date-picker/dist/tiny-date-picker.css';
// import tinyDatePicker from 'tiny-date-picker';

import {waitForTimerange} from './clientsocket.js';
import {
	dragBox,
	draw,
	drawSource,
	lineSource,
	// Map,
	polySource,
	setSearchAreaFromSelected,
} from './map.js';

import {
	hiddenStyle,
	vesselStyles,
	// Vessellabels,
	vesseltypes,
} from './palette.js';

let map = null;
async function mapHook(hook) {
	map = hook;
}

/** @constant {element} statusdiv status message div element */
const statusdiv = document.querySelector('#status-div');
/** @constant {element} selectbtn select area button */
const selectbtn = document.querySelector('#selectbtn');
/** @constant {element} timeselectstart time start date input */
const timeselectstart = document.querySelector('#time-select-start');
/** @constant {element} timeselectend time end date input */
const timeselectend = document.querySelector('#time-select-end');
/** @constant {element} vesseltypeselect filter by vessel type interaction */
const vesseltypeselect = document.querySelector('#vesseltype-select');
/** @constant {element} searchbtn start new database search button */
const searchbtn = document.querySelector('#searchbtn');
/** @constant {element} clearbtn map window reset button */
const clearbtn = document.querySelector('#clearbtn');
/** @constant {element} selectmenu area selection popup menu element */
const selectmenu = document.querySelector('#select-menu');
/** @constant {element} vesselmenu vessel type selection popup menu element */
const vesselmenu = document.querySelector('#vesseltype-menu');

// Let socket = null;
// window.dbsocket = socket;

window.timeselectstart = timeselectstart;
/*
TinyDatePicker({ input:document.getElementById('time-select-start') });
tinyDatePicker({ input:document.getElementById('time-select-end') });
*/
const timeselectstart_fp = flatpickr(timeselectstart, {
	onChange(selectedDates, dateString, instance) {
		timeselectstart.value = dateString;
	},
});
const timeselectend_fp = flatpickr(timeselectend, {
	onChange(selectedDates, dateString, instance) {
		timeselectend.value = dateString;
	},
});

/** Searchstate true if not currently performing a search
 * @see resetSearchState
 * @type {boolean}
 */
let searchstate = true;

/** Reset the search state
 * @see searchstate
 */
async function resetSearchState() {
	searchstate = true;
}

/** Awaits until searchstate is true
 * @see searchstate
 */
async function waitForSearchState() {
	while (searchstate === false) {
		await new Promise(resolve => setTimeout(resolve, 10));
	}
}

/** Cancel button action */
async function cancelSearch(db_socket) {
	searchbtn.textContent = 'Search';
	statusdiv.textContent = 'Cancelled search';
	window.statusmsg = statusdiv.textContent;
	document.body.style.cursor = 'initial';
	await resetSearchState();
	await db_socket.send(JSON.stringify({type: 'stop'}));
}

/** Initiate new database query from server via socket
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
async function newSearch(start, end, db_socket) {
	searchstate = false;
	statusdiv.textContent = 'Searching...';
	searchbtn.textContent = 'Cancel';
	window.statusmsg = statusdiv.textContent;
	let type = 'track_vectors';
	if (window.heatmaptest === true) {
		type = 'heatmap';
		console.log('debugging heatmap...');
	}

	await db_socket.send(JSON.stringify({
		type,
		start,
		end,
		area: window.searcharea,
	}));
	window.searcharea = null;
}

/** Select button click action
 * @callback selectbtn_onclick
 * @function
 */
selectbtn.addEventListener('click', () => {
	// Selectmenu.classList.toggle('show');
	polySource.clear();
	map.removeInteraction(draw);
	map.removeInteraction(dragBox);
	drawSource.clear();
	map.addInteraction(draw);
	map.addInteraction(dragBox);
	document.body.style.cursor = 'crosshair';
});

/** Select menu options click actions
 * @callback selectmenu_childNodes_onclick
 * @function
 */
/*
selectmenu.childNodes.forEach((opt) => {
  opt.onclick = async function() {
    selectmenu.classList.toggle('show');
    if (opt.dataset.value === 'ecoregions' &&
      polySource.getFeatures().length === 0) {
      map.removeInteraction(draw);
      map.removeInteraction(dragBox);
      document.body.style.cursor = 'grab';
      drawSource.clear();
      await socket.send(JSON.stringify({ type: 'zones' }));
    } else if (opt.dataset.value === 'selectbox') {
      polySource.clear();
      map.removeInteraction(draw);
      map.removeInteraction(dragBox);
      document.body.style.cursor = 'crosshair';
      drawSource.clear();
      map.addInteraction(draw);
      map.addInteraction(dragBox);
    }
  };
});
*/

/** search button click action
 * @callback searchbtn_onclick
 * @function
 */
searchbtn.addEventListener('click', async () => {
	const start = timeselectstart.value;
	const end = timeselectend.value;

	await waitForTimerange();

	// Validate input and create database request
	if (searchstate === false) {
		// If already searching, send STOP
		await cancelSearch(window.socket);
	} else if (window.searcharea === null) {
		// Validate area input
		statusdiv.textContent = 'Error: No area selected';
		window.statusmsg = statusdiv.textContent;
	} else if (start === '' || end === '') {
		// Validate time input
		statusdiv.textContent = 'Error: No time selected';
		window.statusmsg = statusdiv.textContent;
	} else if (start >= end) {
		// Validate time input
		statusdiv.textContent = 'Error: Start must occur before end';
		window.statusmsg = statusdiv.textContent;
	} else if (start < timeselectstart_fp.config.minDate) {
		// Validate time input
		statusdiv.textContent = `Error: No data before ${timeselectstart.min}`;
		window.statusmsg = statusdiv.textContent;
	} else if (end > timeselectend_fp.config.maxDate) {
		// Validate time input
		statusdiv.textContent = `Error: No data after ${timeselectend.max}`;
		window.statusmsg = statusdiv.textContent;
	} else if (Math.floor((new Date(end) - new Date(start)) / (1000 * 60 * 60 * 24)) > 31) {
		// Validate time input
		statusdiv.textContent = 'Error: select a time range of one month or less';
		window.statusmsg = statusdiv.textContent;
	} else if (searchstate === true) {
		// Create database request if everything is OK
		await newSearch(start, end, window.socket);
	}

	map.removeInteraction(draw);
	map.removeInteraction(dragBox);
	document.body.style.cursor = 'initial';
});

/** Reset button click action
 * @callback clearbtn_onclick
 * @function
 */
clearbtn.addEventListener('click', async () => {
	selectbtn.textContent = 'Select Area';
	await setSearchAreaFromSelected();
	window.statusmsg = '';
	statusdiv.textContent = '';
	if (searchstate === false) {
		await cancelSearch(window.socket);
	}

	map.removeInteraction(draw);
	map.removeInteraction(dragBox);
	document.body.style.cursor = 'initial';
	drawSource.clear();
	lineSource.clear();
});

/** Set date input min/max values upon connection to server.
 * a timerange request is made on connection, and this function will be
 * called
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
function setSearchRange(start, end) {
	timeselectstart_fp.set('minDate', start);
	timeselectend_fp.set('minDate', start);
	timeselectstart_fp.set('maxDate', end);
	timeselectend_fp.set('maxDate', end);

	if (timeselectstart.value !== undefined && timeselectstart.value !== null && timeselectstart.value !== '') {
		// Console.log(timeselectstart.value_);
		return;
	}

	// Halfway point between start and end
	const defaultStart = new Date(
		(new Date(start).getTime() + new Date(end).getTime()) / 2)
		.toISOString().split('T')[0];

	// Start point plus two weeks
	const defaultEnd = new Date(
		(new Date(start).getTime() + new Date(end).getTime()) / 2 + 12_096e5)
		.toISOString().split('T')[0];

	if (timeselectstart.value === '' && timeselectend.value === '') {
		timeselectstart_fp.set('defaultDate', defaultStart);
		timeselectend_fp.set('defaultDate', defaultEnd);
		timeselectstart_fp.jumpToDate(defaultStart, true);
		timeselectend_fp.jumpToDate(defaultEnd, true);

		timeselectstart.value = defaultStart;
		timeselectend.value = defaultEnd;
	}
}

/** Set start/end date input default values. used by module:url for setting
 * query time via GET request.
 * @param {string} start start time as retrieved from date input, e.g.
 * 2021-01-01
 * @param {string} end end time as retrieved from date input, e.g. 2021-01-01
 */
function setSearchValue(start, end) {
	// Timeselectstart_fp.set('defaultDate', start);
	// timeselectstart_fp.jumpToDate(start, true);
	timeselectstart.value = start;
	// Timeselectend_fp.set('defaultDate', end);
	// timeselectend_fp.jumpToDate(end, true);
	timeselectend.value = end;
}

/** SelectedType
 * @type {String}
 */
let selectedType = 'All';

/** Set feature style according to vessel type and currently displayed types
 * @param {ol.feature.Feature} ft track feature to be styled according to
 * current selectedType
 * @see selectedType
 */
function set_track_style(ft) {
	if (selectedType === 'All' || ft.get('meta').vesseltype_generic.includes(selectedType)) {
		ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
	} else {
		ft.setStyle(hiddenStyle);
	}
}

/** Update all track features in lineSource according to current selectedType
 * @param {ol.source.Vector} lineSource target layer
 * @see selectedType
 */
function update_vesseltype_styles(lineSource) {
	/* Vessel types selector action */
	if (selectedType === 'All') {
		for (const ft of lineSource.getFeatures()) {
			ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
		}
	} else {
		for (const ft of lineSource.getFeatures()) {
			if (ft.get('meta').vesseltype_generic.includes(selectedType)) {
				ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
			} else {
				ft.setStyle(hiddenStyle);
			}
		}
	}
}

/** Add a new item to the vesseltype selection popup menu
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

export {
	clearbtn,
	createVesselMenuItem,
	mapHook,
	resetSearchState,
	searchbtn,
	setSearchRange,
	setSearchValue,
	set_track_style,
	update_vesseltype_styles,
	vesselmenu,
	vesseltypeselect,
	waitForSearchState,
};
