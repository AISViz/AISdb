import socket from "./clientsocket"

let statusdiv = document.getElementById('status-div');

async function requestZones() {
  await socket.send(JSON.stringify({"type": "zones"}));
}
async function requestTracks(date) {
  await socket.send(JSON.stringify({"type": "tracks_week", "date": date}));
}


let searchbtn = document.getElementById('searchbtn');
let searchstate = true;

searchbtn.onclick = function() {
  if (searchstate === true) {
    var week = document.getElementById('time-select').value;
    var dateinput = week.split('-W');
    var reqdate = new Date(dateinput[0], 0, (1 + (dateinput[1]-1) * 7)).toJSON().slice(0,10);
    statusdiv.textContent = `Searching...`;
    requestTracks(reqdate);
    searchbtn.textContent = 'Stop';
    searchstate = false;
  }
  else {
    socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
    searchbtn.disabled = true;
  }
}


let clearbtn = document.getElementById('clearbtn');

clearbtn.onclick = function() {
  //map.layers = [];
  document.getElementById('status-div').textContent = '';
  if (searchstate === false) {
    socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
  }
}

export { searchbtn, clearbtn };
