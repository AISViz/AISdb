import 'ol/ol.css';
import * as olProj from 'ol/proj';
import { Map as _Map } from 'ol';
import BingMaps from 'ol/source/BingMaps';
import TileLayer from 'ol/layer/Tile';
import View from 'ol/View';
import WKB from 'ol/format/WKB';
import GeoJSON from 'ol/format/GeoJSON';
import { Vector as VectorSource } from 'ol/source';
import { Vector as VectorLayer } from 'ol/layer';
import Draw, { createBox } from 'ol/interaction/Draw';
import { DragBox, Select } from 'ol/interaction';
import Feature from 'ol/Feature';

import {
  polyStyle,
  selectStyle,
  vesselStyles,
  vesseltypes,
} from './palette';

import { set_track_style } from './selectform';


const statusdiv = document.getElementById('status-div');


let mapLayer = new TileLayer({
  // visible: true,
  // preload: Infinity,
  source: new BingMaps({
    key: import.meta.env.VITE_BINGMAPSKEY,
    imagerySet: 'Aerial',
    // use maxZoom 19 to see stretched tiles instead of the BingMaps
    // "no photos at this zoom level" tiles
    // maxZoom: 19
  }),
  // zIndex: 0,
});
/*
import OSM from 'ol/source/OSM';
let mapLayer = new TileLayer({
  visible: true,
  source: new OSM(),
});
*/


const drawSource = new VectorSource({ wrapX: false });
const drawLayer = new VectorLayer({ source: drawSource, zIndex: 10 });

const polySource = new VectorSource({});
const polyLayer = new VectorLayer({
  source: polySource,
  style: polyStyle, zIndex: 1,
});
const lineSource = new VectorSource({});
const lineLayer = new VectorLayer({
  source: lineSource,
  style: vesselStyles.Unspecified,
  zIndex: 2,
});


let mapview = new View({
  center: olProj.fromLonLat([ -63.6, 44.0 ]), // east
  // center: olProj.fromLonLat([-123.0, 49.2]), //west
  // center: olProj.fromLonLat([ -100, 57 ]), // canada
  zoom: 7,
});
let map = new _Map({
  target: 'mapDiv', // div item in index.html
  layers: [ mapLayer, polyLayer, lineLayer, drawLayer ],
  view: mapview,
});

/*
let map = new _Map();
map.setLayers([ mapLayer, polyLayer, lineLayer, drawLayer ]);
map.setView(mapview);
map.setTarget('mapDiv');
*/

/* map interactions */
window.searcharea = null;

/* cursor styling: indicate to the user that we are selecting an area */
/*
let draw = null; // global so we can remove it later
function addInteraction() {
  draw = new Draw({
    //source: drawSource,
    //type: 'Circle',
    //geometryFunction: createBox(),
    //geometryName: 'selectbox',
    //zIndex: 10,
  });
  map.addInteraction(draw);
}
*/
let draw = new Draw({
  type: 'Point',
});

const dragBox = new DragBox({});
dragBox.on('boxend', () => {
  window.geom = dragBox.getGeometry();
  let selectFeature = new Feature({
    geometry: dragBox.getGeometry(),
    name: 'selectionArea',
  });
  map.removeInteraction(dragBox);
  drawSource.addFeature(selectFeature);
});
function addInteraction() {
  map.addInteraction(draw);
  map.addInteraction(dragBox);
}

drawSource.on('addfeature', (e) => {
  let selectbox = drawSource.getFeatures()[0].getGeometry().clone()
    .transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
  let minX = Math.min(selectbox[0][0], selectbox[1][0],
    selectbox[2][0], selectbox[3][0], selectbox[4][0]);
  let maxX = Math.max(selectbox[0][0], selectbox[1][0],
    selectbox[2][0], selectbox[3][0], selectbox[4][0]);
  let minY = Math.min(selectbox[0][1], selectbox[1][1],
    selectbox[2][1], selectbox[3][1], selectbox[4][1]);
  let maxY = Math.max(selectbox[0][1], selectbox[1][1],
    selectbox[2][1], selectbox[3][1], selectbox[4][1]);
  window.searcharea = { minX:minX, maxX:maxX, minY:minY, maxY:maxY };
  map.removeInteraction(draw);
  map.removeInteraction(dragBox);
});


function clearFeatures() {
  /** clear all geometry features from map */
  drawSource.clear();
  lineSource.clear();
  polySource.clear();
}

function newTrackFeature(geojs, meta) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857',
  });
  let meta_str = '';
  if (meta.mmsi !== 'None') {
    meta_str = `${meta_str}MMSI: ${meta.mmsi}&emsp;`;
  }
  if (meta.imo !== 'None' && meta.imo !== 0) {
    meta_str = `${meta_str}IMO: ${meta.imo}&emsp;`;
  }
  if (meta.name !== 'None' && meta.name !== 0) {
    meta_str = `${meta_str}name: ${meta.name}&emsp;`;
  }
  if (meta.vesseltype_generic !== 'None') {
    meta_str = `${meta_str}type: ${meta.vesseltype_generic}&ensp;`;
  }
  if (
    meta.vesseltype_detailed !== 'None' &&
    meta.vesseltype_generic !== meta.vesseltype_detailed
  ) {
    meta_str = `${meta_str }(${meta.vesseltype_detailed})&emsp;`;
  }
  if (meta.flag !== 'None') {
    meta_str = `${meta_str }flag: ${meta.flag}  `;
  }
  feature.setProperties({
    meta: meta,
    meta_str: meta_str.replace(' ', '&nbsp;'),
  });
  set_track_style(feature);
  feature.set('COLOR', vesseltypes[meta.vesseltype_generic]);
  lineSource.addFeature(feature);
}

function newPolygonFeature(geojs, meta) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857',
  });
  feature.setProperties({ meta_str: meta.name });
  polySource.addFeature(feature);
}


function layerFilterCallback(l) {
  if (l === lineLayer || l === polyLayer) {
    return true;
  }
  return false;
}

let selected = null;
let previous = null;
map.on('pointermove', (e) => {
  if (selected !== null) {
    selected.setStyle(undefined);
    selected = null;
  }

  // reset track style to previous un-highlighted color
  if (previous !== null &&
    previous !== selected &&
    previous.get('meta') !== undefined) {
    // previous.setStyle(vesselStyles[previous.get('meta').vesseltype_generic]);
    set_track_style(previous);
  }

  // highlight feature at cursor
  map.forEachFeatureAtPixel(e.pixel, (f) => {
    selected = f;
    selectStyle
      .getFill()
      .setColor(f.get('COLOR') || 'rgba(255, 255, 255, 0.45)');
    f.setStyle(selectStyle);

    // keep track of last feature so that styles can be reset after moving mouse
    if (previous === null || previous.get('meta_str') !== f.get('meta_str')) {
      previous = f;
    }
    return true;
  }, { layerFilter: layerFilterCallback }
  );

  // show metadata for selected feature
  if (selected) {
    statusdiv.innerHTML = selected.get('meta_str');
  } else {
    statusdiv.innerHTML = window.statusmsg;
  }
});


export {
  addInteraction,
  clearFeatures,
  draw,
  dragBox,
  drawSource,
  lineSource,
  map,
  mapview,
  newPolygonFeature,
  newTrackFeature
};
