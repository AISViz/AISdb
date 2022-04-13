import 'ol/ol.css';
import * as olProj from 'ol/proj';
import Map from 'ol/Map';
import BingMaps from 'ol/source/BingMaps';
import TileLayer from 'ol/layer/Tile';
import View from 'ol/View';
import WKB from 'ol/format/WKB';
import TopoJSON from 'ol/format/TopoJSON';
import GeoJSON from 'ol/format/GeoJSON';
import {Vector as VectorSource} from 'ol/source';
import {Tile, Vector} from 'ol/layer';
import {Fill, Stroke, Style, Text} from 'ol/style';

/* RUST HOOK */
//import {process_response} from "../pkg/client";
import vesseltypes from './palette'

const select = document.getElementById('layer-select');
const statusdiv = document.getElementById('status-div');

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
  if (opts['color'] === undefined) { opts['color'] = '#FFFFFF'; }
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

function newGeoVectorLayer(geojs, meta) { 
  //let obj = {"x":[1., 2., 3], "y":[3,4,5], "t":[1], "meta": {"mmsi":"1", "ship_type": "2"}};
  //let test = js2bin(obj);
  //let response = process_response({'rawdata':test});
  //console.log(JSON.stringify(response));
  //console.log(JSON.stringify(meta));
  let opts = defaultOptions({ 'color': vesseltypes[meta['vesseltype_generic']] });
  
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857',
  });
  let meta_str = '';
  if (meta['mmsi'] != 'None') {
    meta_str += `MMSI: ${meta['mmsi']}&emsp;`
  }
  if (meta['imo'] != 'None') {
    meta_str += `IMO: ${meta['imo']}&emsp;`
  }
  if (meta['name'] != 'None') {
    meta_str += `name: ${meta['name']}&emsp;`
  }
  if (meta['vesseltype_generic'] != 'None') {
    meta_str += `type: ${meta['vesseltype_generic']}&ensp;`;
  }
  if (meta['vesseltype_detailed'] != 'None' && meta['vesseltype_generic'] != meta['vesseltype_detailed']) {
    meta_str += `(${meta['vesseltype_detailed']})&emsp;`;
  }
  if (meta['flag'] != 'None') {
    meta_str += `flag: ${meta['flag']}  `
  }
  feature.setProperties({'meta':meta_str});
  const vector = new Vector({
    source: new VectorSource({
      features: [feature],
    }),
    declutter: opts['declutter'],
    opacity: opts['opacity'],
    style: opts['style'],
    zIndex: opts['zIndex'],
  });
  map.addLayer(vector);
}

function newWKBHexVectorLayer(wkbFeatures, meta) { 
  //opts = defaultOptions(opts);
  let opts = defaultOptions({ 'color': '#000000' });
  const format = new WKB();
  var features = [];
  for (let i = 0, ii = wkbFeatures.length; i < ii; ++i) {
    const feature = format.readFeature(
      wkbFeatures[i], 
      { dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857',
      }
    );
    feature.setProperties({meta:meta['label']})
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


/* on mousever status info */
const selectStyle = new Style({
  fill: new Fill({
    color: '#eeeeee',
    //color: 'rgba(255, 255, 255, 0.4)',
  }),
  stroke: new Stroke({
    color: 'rgba(255, 255, 255, 0.7)',
    width: 4,
  }),
});

let selected = null;
map.on('pointermove', function (e) {
  if (selected !== null) {
    selected.setStyle(undefined);
    selected = null;
  }

  map.forEachFeatureAtPixel(e.pixel, function (f) {
    selected = f;
    selectStyle.getFill().setColor(f.get('COLOR') || 'rgba(255, 255, 255, 0.45)');
    f.setStyle(selectStyle);
    return true;
  });

  if (selected) {
    statusdiv.innerHTML = selected.get('meta');
  } else {
    statusdiv.innerHTML = window.statusmsg;
  }
});

export {newWKBHexVectorLayer, newGeoVectorLayer, vesseltypes};
