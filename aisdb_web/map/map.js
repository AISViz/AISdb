import "ol/ol.css";
import * as olProj from "ol/proj";
import Map from "ol/Map";
import BingMaps from "ol/source/BingMaps";
import TileLayer from "ol/layer/Tile";
import View from "ol/View";
import WKB from "ol/format/WKB";
import TopoJSON from "ol/format/TopoJSON";
import GeoJSON from "ol/format/GeoJSON";
import { Vector as VectorSource } from "ol/source";
import { Tile, Vector as VectorLayer } from "ol/layer";
import { Fill, Stroke, Style, Text } from "ol/style";
import Draw, { createBox, createRegularPolygon } from "ol/interaction/Draw";

import {vesseltypes, polyStyle, selectStyle, vesselStyles} from "./palette";


const statusdiv = document.getElementById("status-div");


let mapLayer = new TileLayer({
  visible: true,
  preload: Infinity,
  source: new BingMaps({
    key: import.meta.env.VITE_BINGMAPSKEY,
    imagerySet: "Aerial",
    // use maxZoom 19 to see stretched tiles instead of the BingMaps
    // "no photos at this zoom level" tiles
    // maxZoom: 19
  }),
  zIndex: 0,
});

/*
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
*/

const drawSource = new VectorSource({ wrapX: false });
const drawLayer = new VectorLayer({ source: drawSource, zIndex: 10 });
let draw; // global so we can remove it later
function addInteraction() {
  draw = new Draw({
    source: drawSource,
    type: "Circle",
    geometryFunction: createBox(),
    geometryName: "selectbox",
    zIndex: 10,
  });
  map.addInteraction(draw);
}
window.searcharea = null;
drawSource.on('addfeature', function(e) {
  let selectbox = drawSource.getFeatures()[0].getGeometry().clone().transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
  let minX = Math.min(selectbox[0][0], selectbox[1][0], selectbox[2][0], selectbox[3][0], selectbox[4][0]);
  let maxX = Math.max(selectbox[0][0], selectbox[1][0], selectbox[2][0], selectbox[3][0], selectbox[4][0]);
  let minY = Math.min(selectbox[0][1], selectbox[1][1], selectbox[2][1], selectbox[3][1], selectbox[4][1]);
  let maxY = Math.max(selectbox[0][1], selectbox[1][1], selectbox[2][1], selectbox[3][1], selectbox[4][1]);
  window.searcharea = {minX:minX, maxX:maxX, minY:minY,maxY:maxY};
  map.removeInteraction(draw);
});

const polySource = new VectorSource({});
const polyLayer = new VectorLayer({source: polySource, style: polyStyle, zIndex: 1,});
const lineSource = new VectorSource({});
const lineLayer = new VectorLayer({source: lineSource, style: vesselStyles['Unspecified'], zIndex: 2,});

function clearFeatures() {
  drawSource.clear();
  lineSource.clear();
  polySource.clear();
}


function newTrackFeature(geojs, meta) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: "EPSG:4326",
    featureProjection: "EPSG:3857",
  });
  let meta_str = "";
  if (meta["mmsi"] != "None") {
    meta_str += `MMSI: ${meta["mmsi"]}&emsp;`;
  }
  if (meta["imo"] != "None") {
    meta_str += `IMO: ${meta["imo"]}&emsp;`;
  }
  if (meta["name"] != "None") {
    meta_str += `name: ${meta["name"]}&emsp;`;
  }
  if (meta["vesseltype_generic"] != "None") {
    meta_str += `type: ${meta["vesseltype_generic"]}&ensp;`;
  }
  if (
    meta["vesseltype_detailed"] != "None" &&
    meta["vesseltype_generic"] != meta["vesseltype_detailed"]
  ) {
    meta_str += `(${meta["vesseltype_detailed"]})&emsp;`;
  }
  if (meta["flag"] != "None") {
    meta_str += `flag: ${meta["flag"]}  `;
  }
  feature.setProperties({ meta: meta, meta_str: meta_str, });
  feature.setStyle(vesselStyles[meta['vesseltype_generic']]);
  feature.set('COLOR', vesseltypes[meta['vesseltype_generic']]);
  lineSource.addFeature(feature);
}

function newPolygonFeature(wkbFeatures, meta) {
  const format = new WKB();
  var features = [];
  for (let i = 0, ii = wkbFeatures.length; i < ii; ++i) {
    const feature = format.readFeature(wkbFeatures[i], {
      dataProjection: "EPSG:4326",
      featureProjection: "EPSG:3857",
    });
    //feature.setGeometryName(meta['label']);
    feature.setProperties({ meta_str: meta["label"] });
    //feature.setId(meta['label']);
    // feature.setStyle(polyStyle);
    polySource.addFeature(feature);
  }
}



var map = new Map({
  target: "map", //div item in index.html
  /*
  layers: [
    new ol.layer.Tile({
      source: new ol.source.OSM()
    })
  ],
  */
  //layers: layers,
  layers: [mapLayer, polyLayer, lineLayer, drawLayer],
  view: new View({
    //center: olProj.fromLonLat([-63.6, 44.6]), //east
    //center: olProj.fromLonLat([-123.0, 49.2]), //west
    center: olProj.fromLonLat([-100, 57]), //west
    zoom: 4,
  }),
});

function layerFilterCallback(l) {
  if (l === lineLayer || l === polyLayer) {
    return true;
  } else {
    return false;
  }
}

let selected = null;
let previous = null;
map.on("pointermove", function (e) {
  if (selected !== null) {
    selected.setStyle(undefined);
    selected = null;
  }

  // reset track style to previous un-highlighted color
  if (previous !== null && previous !== selected && previous.get('meta') !== undefined) {
    previous.setStyle(vesselStyles[previous.get('meta')['vesseltype_generic']]);
  }

  // highlight feature at cursor
  map.forEachFeatureAtPixel(e.pixel, function (f) {
    selected = f;
    selectStyle
      .getFill()
      .setColor(f.get("COLOR") || "rgba(255, 255, 255, 0.45)");
    f.setStyle(selectStyle);

    // keep track of last feature so that styles can be reset after moving mouse
    if (previous === null || previous.get('meta_str') !== f.get('meta_str')) {
      previous = f;
    }
    return true;
  }, {layerFilter: layerFilterCallback}
  );

  // show metadata for selected feature
  if (selected) {
    statusdiv.innerHTML = selected.get("meta_str");
  } else {
    statusdiv.innerHTML = window.statusmsg;
  }
});

export { map, draw, addInteraction, clearFeatures, drawSource, newPolygonFeature, newTrackFeature };
