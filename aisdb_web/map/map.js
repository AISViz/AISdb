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
    //center: olProj.fromLonLat([-63.6, 44.6]),
    center: olProj.fromLonLat([-123.0, 49.2]),
    zoom: 4.5
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
//window.newTopoVectorLayer = newTopoVectorLayer;
//window.newWKBHexVectorLayer = newWKBHexVectorLayer;
//window.zones = requestZones;
//window.demo_7day = requestTracks;

export {map, newWKBHexVectorLayer, newTopoVectorLayer};
