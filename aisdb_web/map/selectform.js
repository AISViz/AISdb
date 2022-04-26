import { socket, waitForTimerange } from './clientsocket';
import {
  addInteraction,
  clearFeatures,
  draw,
  dragBox,
  drawSource,
  lineSource,
  map,
} from './map';

import { vessellabels, vesseltypes, vesselStyles, hiddenStyle } from './palette';


let statusdiv = document.getElementById('status-div');

const selectbtn = document.getElementById('selectbtn');
const timeselectstart = document.getElementById('time-select-start');
const timeselectend = document.getElementById('time-select-end');
const vesseltypeselect = document.getElementById('vesseltype-select');
const searchbtn = document.getElementById('searchbtn');
const clearbtn = document.getElementById('clearbtn');

selectbtn.onclick = function () {
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
  drawSource.clear();
  addInteraction();
};


let searchstate = true;
window.searchstate = function() {
  console.log(searchstate);
};

async function resetSearchState() {
  searchstate = true;
}

async function waitForSearchState() {
  while (searchstate === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 250);
    });
  }
}

async function cancelSearch() {
  searchbtn.disabled = true;
  searchbtn.textContent = 'Search';
  statusdiv.textContent = 'Cancelling...';
  window.statusmsg = statusdiv.textContent;
  await socket.send(JSON.stringify({ type: 'stop' }));
  await waitForSearchState();
}

async function newSearch(start, end) {
  searchstate = false;
  statusdiv.textContent = 'Searching...';
  searchbtn.textContent = 'Cancel';
  window.statusmsg = statusdiv.textContent;
  await socket.send(JSON.stringify({
    type: 'track_vectors',
    start: start,
    end: end,
    area: window.searcharea,
  }));
  window.searcharea = null;
  await waitForSearchState();
  drawSource.clear();
}

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
  } else if (start < timeselectstart.min) {
    // validate time input
    statusdiv.textContent = `Error: No data before ${timeselectstart.min}`;
    window.statusmsg = statusdiv.textContent;
  } else if (end > timeselectend.max) {
    // validate time input
    statusdiv.textContent = `Error: No data after ${timeselectend.max}`;
    window.statusmsg = statusdiv.textContent;
  } else if (searchstate === true) {
    // create database request if everything is OK
    await newSearch(start, end);
  }
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
};


clearbtn.onclick = async function() {
  window.searcharea = null;
  window.statusmsg = '';
  statusdiv.textContent = '';
  if (searchstate === false) {
    await cancelSearch();
  }
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
  clearFeatures();
};


async function setSearchRange(start, end) {
  /* set min/max time range values */
  timeselectstart.min = start;
  timeselectend.min = start;
  timeselectstart.max = end;
  timeselectend.max = end;
}

async function setSearchValue(start, end) {
  timeselectstart.value = start;
  timeselectend.value = end;
}

const vesselmenu = document.getElementById('vesseltype-menu');
let selectedType = 'All';

function set_track_style(ft) {
  /* set feature style according to vessel type and currently displayed types */
  if (selectedType === 'All') {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else if (ft.get('meta').vesseltype_generic.includes(selectedType)) {
    ft.setStyle(vesselStyles[ft.get('meta').vesseltype_generic]);
  } else {
    ft.setStyle(hiddenStyle);
  }
}

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

// vesseltypeselect.addEventListener('change', update_vesseltype_styles);
function createVesselMenuItem(label, value, symbol) {
  if (symbol === undefined) {
    // symbol = '⚫';
    // symbol = 'X';
    symbol = '■';
  }
  let opt = document.createElement('a');
  let colordot = `<div class="colordot" style="color: rgb(${vesseltypes[label]}); display: inline-block;">${symbol}</div>`;
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
// '⚪'
// createVesselMenuItem('Unknown', 'None', 'x');
createVesselMenuItem('Unknown', 'None', '□');
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
