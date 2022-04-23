import socket from './clientsocket';
import {
  addInteraction,
  clearFeatures,
  draw,
  drawSource,
  lineSource,
  map,
} from './map';

import { vessellabels, vesselStyles, hiddenStyle } from './palette';


let statusdiv = document.getElementById('status-div');

const selectbtn = document.getElementById('selectbtn');
const timeselectstart = document.getElementById('time-select-start');
const timeselectend = document.getElementById('time-select-end');
const vesseltypeselect = document.getElementById('vesseltype-select');
const searchbtn = document.getElementById('searchbtn');
const clearbtn = document.getElementById('clearbtn');

selectbtn.onclick = function () {
  map.removeInteraction(draw);
  drawSource.clear();
  addInteraction();
};


let searchstate = true;
searchbtn.onclick = async function() {
  let start = document.getElementById('time-select-start').value;
  let end = document.getElementById('time-select-end').value;

  // validate input and create database request
  if (window.searcharea === null) {
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
  } else if (start < timeselectstart.min) {
    // validate time input
    statusdiv.textContent = `Error: No data before ${timeselectstart.min}`;
    window.statusmsg = statusdiv.textContent;
  } else if (end > timeselectend.max) {
    // validate time input
    statusdiv.textContent = `Error: No data after ${timeselectend.max}`;
    window.statusmsg = statusdiv.textContent;
  } else if (searchstate !== true) {
    // if already searching, send STOP
    await socket.send(JSON.stringify({ type: 'stop' }));
    searchbtn.textContent = 'Search';
    searchstate = true;
    searchbtn.disabled = true;
  } else if (searchstate === true) {
    // create database request if everything is OK
    statusdiv.textContent = 'Searching...';
    window.statusmsg = statusdiv.textContent;
    await socket.send(JSON.stringify({
      type: 'track_vectors',
      start: start,
      end: end,
      area: window.searcharea,
    }));
    searchbtn.textContent = 'Stop';
    searchstate = false;
    drawSource.clear();
  }
  map.removeInteraction(draw);
};


clearbtn.onclick = async function() {
  // map.layers = [];
  searchbtn.disabled = true;
  window.searcharea = null;
  window.statusmsg = '';
  statusdiv.textContent = '';
  if (searchstate === false) {
    // statusdiv.textContent = 'Stopping...';
    // window.statusmsg = statusdiv.textContent;
    await socket.send(JSON.stringify({ type: 'stop' }));
    searchbtn.textContent = 'Search';
    searchstate = true;
  }
  map.removeInteraction(draw);
  clearFeatures();
  searchbtn.disabled = false;
};


function setSearchRange(start, end) {
  /* set min/max time range values */
  timeselectstart.min = start;
  timeselectend.min = start;
  timeselectstart.max = end;
  timeselectend.max = end;
}

for (let label of vessellabels) {
  let opt = document.createElement('option');
  opt.value = label;
  opt.innerHTML = label;
  vesseltypeselect.appendChild(opt);
}
let opt = document.createElement('option');
opt.value = 'None';
opt.innerHTML = 'Unknown';
vesseltypeselect.appendChild(opt);

function set_track_style(ft) {
  /* set feature style according to vessel type and currently displayed types */
  if (vesseltypeselect.value === 'All') {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else if (ft.get('meta').vesseltype_generic.includes(vesseltypeselect.value)) {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else {
    ft.setStyle(hiddenStyle);
  }
}

function update_vesseltype_styles() {
  /* vessel types selector action */
  if (vesseltypeselect.value === 'All') {
    for (let ft of lineSource.getFeatures()) {
      ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
    }
  } else {
    for (let ft of lineSource.getFeatures()) {
      if (ft.get('meta').vesseltype_generic.includes(vesseltypeselect.value)) {
        ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
      } else {
        ft.setStyle(hiddenStyle);
      }
    }
  }
}
vesseltypeselect.addEventListener('change', update_vesseltype_styles);

// const downloadbtn = document.getElementById('downloadbtn');
// downloadbtn.style.display = 'none';

export {
  clearbtn,
  searchbtn,
  setSearchRange,
  set_track_style,
};
