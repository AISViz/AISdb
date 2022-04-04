import 'ol/ol.css';
import * as olProj from 'ol/proj';
import Map from 'ol/Map';
import BingMaps from 'ol/source/BingMaps';
import TileLayer from 'ol/layer/Tile';
import View from 'ol/View';
import WKB from 'ol/format/WKB';
import TopoJSON from 'ol/format/TopoJSON';
import {Vector as VectorSource} from 'ol/source';
import {Tile, Vector} from 'ol/layer';
import {Fill, Stroke, Style, Text} from 'ol/style';


const styles = [
  'Aerial',
  'AerialWithLabelsOnDemand',
  'CanvasDark',
  'RoadOnDemand',
];


const layers = [];
let i, ii;
for (i = 0, ii = styles.length; i < ii; ++i) {
  layers.push(
    new TileLayer({
      visible: false,
      preload: Infinity,
      source: new BingMaps({
        key: import.meta.env.VITE_BINGMAPSKEY,
        imagerySet: styles[i],
        // use maxZoom 19 to see stretched tiles instead of the BingMaps
        // "no photos at this zoom level" tiles
        // maxZoom: 19
      }),
    })
  );
}


const select = document.getElementById('layer-select');
function onChange() {
  const style = select.value;
  for (let i = 0, ii = layers.length; i < ii; ++i) {
    layers[i].setVisible(styles[i] === style);
  }
}
select.addEventListener('change', onChange);
onChange();

var map = new Map({
  target: 'map', //div item in index.html
  /*
  layers: [
    new ol.layer.Tile({
      source: new ol.source.OSM()
    })
  ],
  */
  layers: layers,
  view: new View({
    //center: ol.proj.fromLonLat([-63.6, 44.6]),
    center: olProj.fromLonLat([-63.6, 44.6]),
    zoom:4 
  })
});

const labelStyle = new Style({
  text: new Text({
    font: '13px Calibri,sans-serif',
    fill: new Fill({
      color: '#000',
    }),
    stroke: new Stroke({
      color: '#fff',
      width: 4,
    }),
  }),
});

function defaultOptions(opts) {
  if (opts['color'] === undefined) { opts['color'] = '#000000'; }
  if (opts['fill'] === undefined) { opts['fill'] = 'rgba(255,255,255,0.3)'; }

  const defaultStroke = new Stroke({
    color: opts['color'],
    width: 1,
  });
  const defaultFill = new Fill({
    color: opts['fill'],
  });
  const defaultStyle = new Style({
    stroke: defaultStroke,
    fill: defaultFill,
  });

  if (opts === undefined) { opts = {}; }

  if (opts['IDs'] === undefined) { opts['IDs'] = undefined; }

  if (opts['opacity'] === undefined) { opts['opacity'] = 1; }

  if (opts['declutter'] === undefined) { opts['declutter'] = false; }

  if (opts['style'] === undefined) { opts['style'] = defaultStyle; }

  return opts;
}


function newTopoVectorLayer(topo, opts) { 
  opts = defaultOptions(opts);
  //const buff = new Buffer(topojsonB64, 'base64');
  const format = new TopoJSON();
  const features = format.readFeatures(topo, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857',
  });
  const vector = new Vector({
    source: new VectorSource({
      features: features,
    }),
    declutter: opts['declutter'],
    opacity: opts['opacity'],
    style: opts['style'],
    zIndex: opts['zIndex'],
  });
  map.addLayer(vector);
}
window.newTopoVectorLayer = newTopoVectorLayer;



function newWKBHexVectorLayer(wkbFeatures, opts) { 
  opts = defaultOptions(opts);
  const format = new WKB();
  var features = [];
  for (let i = 0, ii = wkbFeatures.length; i < ii; ++i) {
    const feature = format.readFeature(
      wkbFeatures[i], 
      { dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857',
      }
    );
    features.push(feature);
  }

  const vector = new Vector({
    source: new VectorSource({
      features: features,
    }),
    declutter: opts['declutter'],
    opacity: opts['opacity'],
    style: opts['style'],
    zIndex: opts['zIndex'],
  });
  map.addLayer(vector);
}
window.newWKBHexVectorLayer = newWKBHexVectorLayer;


window.sample_wkb = '0103000000010000000500000054E3A59BC4602540643BDF4F8D1739C05C8FC2F5284C4140EC51B81E852B34C0D578E926316843406F1283C0CAD141C01B2FDD2406012B40A4703D0AD79343C054E3A59BC4602540643BDF4F8D1739C0';

//const socketHost = 'ws://localhost:9924';
//import.meta.env.VITE_BINGMAPSKEY
let hostname = import.meta.env.VITE_AISDBHOST;
if (hostname == undefined) {
  hostname = 'localhost';
}
let port = import.meta.env.VITE_AISDBPORT;
if (port == undefined) {
  port = '9924';
}
const socketHost = `ws://${hostname}:${port}`
let sock = new WebSocket(socketHost);

sock.onopen = function(event) {
  console.log(`Established connection to ${socketHost}\nCaution: connection is unencrypted!`);
}
sock.onclose = function(event) {
  if (event.wasClean) {
    console.log(`[${event.code}] Closed connection with ${socketHost}`);
  } else {
    console.log(`[${event.code}] Connection to ${socketHost} died unexpectedly`);
  }
}
sock.onerror = function(error) {
  console.log(`[${error.code}] ${error.message}`);
  sock.close();
}




sock.onmessage = async function(event) {
  let response = JSON.parse(event.data);
  window.last = response;
  if (response['type'] === 'WKBHex') {
    newWKBHexVectorLayer([response['geometry']], response['opts']);
  } else if (response['type'] === 'topology') {
    newTopoVectorLayer(response['topology'], response['opts']);
  }
}


window.onbefureunload = function() {
  sock.onclose = function() {} ;
  sock.close();
}
async function requestZones() {
  await sock.send(JSON.stringify({"type": "zones"}));
}
window.zones = requestZones;

async function requestTracks(month) {
  await sock.send(JSON.stringify({"type": "tracks_month", "month": month}));
}
window.demo = requestTracks;

