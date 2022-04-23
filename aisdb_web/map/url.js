import socket from './clientsocket';

const urlParams = new URLSearchParams(window.location.search);

function parseUrl() {
  if (urlParams.get('ecoregions') !== undefined &&
    urlParams.get('ecoregions') !== null) {
    socket.send(JSON.stringify({ type: 'zones' }));
  }
}

export default parseUrl;
