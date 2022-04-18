import socket from "./clientsocket"
import {map, draw, drawSource, addInteraction, clearFeatures} from "./map"

let statusdiv = document.getElementById('status-div');

async function requestZones() {
  await socket.send(JSON.stringify({"type": "zones"}));
}

const selectbtn = document.getElementById("selectbtn");
//selectbtn.style.display = "None";
selectbtn.onclick = function () {
  map.removeInteraction(draw);
  drawSource.clear();
  addInteraction();
}


const searchbtn = document.getElementById('searchbtn');
let searchstate = true;
searchbtn.onclick = async function() {
  var start = document.getElementById('time-select-start').value;
  var end = document.getElementById('time-select-end').value;
  if ( window.searcharea === null ) {
    statusdiv.textContent = 'Error: No area selected'
    window.statusmsg = statusdiv.textContent
  } else if (start === '' || end === '') {
    statusdiv.textContent = 'Error: No time selected'
    window.statusmsg = statusdiv.textContent
  } else if (searchstate === true) {
    statusdiv.textContent = `Searching...`;
    window.statusmsg = statusdiv.textContent;
    await socket.send(JSON.stringify({"type": "track_vectors", "start": start, "end": end, area: window.searcharea,}));
    searchbtn.textContent = 'Stop';
    searchstate = false;
    drawSource.clear();
  } else {
    await socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
    searchbtn.disabled = true;
  }
  map.removeInteraction(draw);
}


const clearbtn = document.getElementById('clearbtn');

clearbtn.onclick = async function() {
  //map.layers = [];
  searchbtn.disabled = true;
  window.searcharea = null;
  window.statusmsg = '';
  statusdiv.textContent = '';
  if (searchstate === false) {
    //statusdiv.textContent = 'Stopping...';
    //window.statusmsg = statusdiv.textContent;
    await socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
  }
  map.removeInteraction(draw);
  clearFeatures();
  searchbtn.disabled = false;
}

const timeselectstart = document.getElementById('time-select-start');
const timeselectend = document.getElementById('time-select-end');

function setSearchRange(start, end) {
  timeselectstart.min = start;
  timeselectend.min = start;
  timeselectstart.max = end;
  timeselectend.max = end;
}


// const downloadbtn = document.getElementById('downloadbtn');
// downloadbtn.style.display = 'none';

export { searchbtn, clearbtn, setSearchRange };
