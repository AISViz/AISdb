import socket from "./clientsocket"

let statusdiv = document.getElementById('status-div');

async function requestZones() {
  await socket.send(JSON.stringify({"type": "zones"}));
}


let searchbtn = document.getElementById('searchbtn');
let searchstate = true;

searchbtn.onclick = async function() {
  if (searchstate === true) {
    var start = document.getElementById('time-select-start').value;
    var end = document.getElementById('time-select-end').value;
    statusdiv.textContent = `Searching...`;
    window.statusmsg = statusdiv.textContent;
    await socket.send(JSON.stringify({"type": "track_vectors", "start": start, "end": end,}));
    searchbtn.textContent = 'Stop';
    searchstate = false;
  }
  else {
    await socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
    searchbtn.disabled = true;
  }
}


let clearbtn = document.getElementById('clearbtn');

clearbtn.onclick = function() {
  //map.layers = [];
  window.statusmsg = '';
  document.getElementById('status-div').textContent = '';
  if (searchstate === false) {
    //socket.send(JSON.stringify({'type': 'stop'}));
    searchbtn.textContent = 'Search';
    searchstate = true;
  }
}

export { searchbtn, clearbtn };
