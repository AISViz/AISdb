/** @module map */
import 'ol/ol.css';
import * as olProj from 'ol/proj';
import { Map as _Map } from 'ol';
import BingMaps from 'ol/source/BingMaps';
import TileLayer from 'ol/layer/Tile';
import View from 'ol/View';
import GeoJSON from 'ol/format/GeoJSON';
import { Vector as VectorSource } from 'ol/source';
import { Vector as VectorLayer } from 'ol/layer';
import Draw from 'ol/interaction/Draw';
import { DragBox } from 'ol/interaction';
import Feature from 'ol/Feature';
import Select from 'ol/interaction/Select';
import { click } from 'ol/events/condition';

import {
  dragBoxStyle,
  polyStyle,
  selectStyle,
  vesselStyles,
  vesseltypes,
} from './palette';

import { set_track_style } from './selectform';


/** status message div item */
const statusdiv = document.getElementById('status-div');


/** ol map TileLayer */
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


/** contains geometry for map selection feature */
const drawSource = new VectorSource({ wrapX: false });
/** contains drawSource for map selection layer */
const drawLayer = new VectorLayer({ source: drawSource, zIndex: 2 });

/** contains geometry for map zone polygons */
const polySource = new VectorSource({});
/** contains polySource for map zone polygons */
const polyLayer = new VectorLayer({
  source: polySource,
  style: polyStyle, zIndex: 1,
});

/** contains map vessel line geometries */
const lineSource = new VectorSource({});
/** contains map lineSource layer */
const lineLayer = new VectorLayer({
  source: lineSource,
  style: vesselStyles.Unspecified,
  zIndex: 3,
});


/** default map position
 * @see module:url
 */
let mapview = new View({
  center: olProj.fromLonLat([ -63.6, 44.0 ]), // east
  // center: olProj.fromLonLat([-123.0, 49.2]), //west
  // center: olProj.fromLonLat([ -100, 57 ]), // canada
  zoom: 7,
});

/** map window
 * @param {string} target target HTML item by ID
 * @param {Array} layers map layers to display
 * @param {ol/View) view default map view positioning
 */
let map = new _Map({
  target: 'mapDiv', // div item in index.html
  layers: [ mapLayer, polyLayer, lineLayer, drawLayer ],
  view: mapview,
});


/* map interactions */
window.searcharea = null;

/* cursor styling: indicate to the user that we are selecting an area */
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
  selectFeature.setStyle(dragBoxStyle);
  drawSource.addFeature(selectFeature);
  map.removeInteraction(dragBox);
});

/** add draw selection box interaction to map */
function addInteraction() {
  map.addInteraction(draw);
  map.addInteraction(dragBox);
}

/** draw layer addfeature event */
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


/** clear all geometry features from map */
function clearFeatures() {
  drawSource.clear();
  lineSource.clear();
}

/** new track geometry feature
 * @param {Object} geojs GeoJSON LineString object
 * @param {Object} meta geometry metadata
 */
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

/** new zone polygon feature
 * @param {Object} geojs GeoJSON Polygon object
 * @param {Object} meta geometry metadata
 */
function newPolygonFeature(geojs, meta) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857',
  });
  feature.setProperties({ meta_str: meta.name });
  polySource.addFeature(feature);
}


/** callback for map pointermove event
 * @param {VectorLayer} l ol VectorLayer
 * @returns {boolean}
 */
function pointermoveLayerFilterCallback(l) {
  if (l === lineLayer || l === polyLayer) {
    return true;
  }
  return false;
}

/** callback for map click event
 * @param {VectorLayer} l ol VectorLayer
 * @returns {boolean}
 */
function clickLayerFilterCallback(l) {
  if (l === polyLayer) {
    return true;
  }
  return false;
}

let selected = null;
let previous = null;
map.on('pointermove', (e) => {
  if (selected !== null && selected.get('selected') !== true) {
    selected.setStyle(undefined);
    selected = null;
  } else
  if (selected !== null) {
    selected = null;
  }

  // reset track style to previous un-highlighted color
  if (previous !== null &&
    previous !== selected &&
    previous.get('meta') !== undefined) {
    set_track_style(previous);
  }

  // highlight feature at cursor
  map.forEachFeatureAtPixel(e.pixel, (f) => {
    selected = f;
    if (f.get('selected') !== true) {
      f.setStyle(selectStyle);
    }

    // keep track of last feature so that styles can be reset after moving mouse
    if (previous === null || previous.get('meta_str') !== f.get('meta_str')) {
      previous = f;
    }
    return true;
  }, { layerFilter: pointermoveLayerFilterCallback }
  );

  // show metadata for selected feature
  if (selected) {
    statusdiv.innerHTML = selected.get('meta_str');
  } else {
    statusdiv.innerHTML = window.statusmsg;
  }
});

/** set a search area bounding box as determined by the extent
 * of currently selected polygons
 */
async function setSearchAreaFromSelected() {
  for (let ft of polySource.getFeatures()) {
    if (ft.get('selected') === true) {
      if (window.searcharea === null) {
        window.searcharea = { minX: 180, maxX:-180, minY:90, maxY:-90 };
      }
      let coords = ft.getGeometry().clone()
        .transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
      for (let point of coords) {
        if (point[0] < window.searcharea.minX) {
          window.searcharea.minX = point[0];
        }
        if (point[0] > window.searcharea.maxX) {
          window.searcharea.maxX = point[0];
        }
        if (point[1] < window.searcharea.minY) {
          window.searcharea.minY = point[1];
        }
        if (point[1] > window.searcharea.maxY) {
          window.searcharea.maxY = point[1];
        }
      }
    }
  }
}
map.on('click', async (e) => {
  map.forEachFeatureAtPixel(e.pixel, async (f) => {
    if (f.get('selected') !== true) {
      f.setStyle(selectStyle);
      f.set('selected', true);
    } else {
      f.setStyle(polyStyle);
      f.set('selected', false);
    }
    window.searcharea = null;
    await setSearchAreaFromSelected();
    return true;
  }, { layerFilter: clickLayerFilterCallback }
  );
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
  newTrackFeature,
  setSearchAreaFromSelected,
};
