import { lineSource } from './map';
import { /* vesselStyles,*/ selectStyle } from './palette.js';

import { Feature } from 'ol';
import { Fill, Stroke, Style, Circle } from 'ol/style';
import { LineString } from 'ol/geom';
// import { Vector as VectorSource } from 'ol/source';
import { fromLonLat } from 'ol/proj';
// import Collection from 'ol/Collection';
// import WebGLPointsLayer from 'ol/layer/WebGLPoints';

/** on mousever feature style */

const ptSelectStyle = function(feature) {
  return new Style({
    image: new Circle({ radius: 4,
      fill: new Fill({
        color: 'rgba(255, 255, 255, 0.85)',
      }),
      stroke: new Stroke({
        // color: 'rgba(10, 80, 255, 0.5)',
        color: 'rgba(100, 100, 100, 0.5)',
        width: 1,
      }) })
  });
};

/* --- */

let streamsocket = new WebSocket('ws://localhost:9920');
let live_targets = {};


streamsocket.onerror = function(event) {
  let msg = `livestream: an unexpected error occurred [${event.code}]`;
  console.log(msg);
  streamsocket.close();
};

streamsocket.onmessage = function (event) {
  // parse message
  let msg = JSON.parse(event.data);

  // create new point from message coordinates
  /*
  let ft = new Feature({
    geometry: new Point(fromLonLat([ msg.lon, msg.lat ])),
    mmsi: msg.mmsi,
  });
  ft.setStyle(ptSelectStyle);
  */

  // check target matrix for ship ID
  let trajectory = live_targets[msg.mmsi];
  let meta_str = `mmsi: ${msg.mmsi}`;
  if (msg.sog >= 0) {
    meta_str = `${meta_str }&emsp;sog: ${msg.sog.toFixed(1)}`;
  }
  if (msg.rot >= 0) {
    meta_str = `${meta_str }&emsp;rot: ${msg.rot.toFixed(1)}`;
  }
  if (msg.heading >= 0) {
    meta_str = `${meta_str }&emsp;heading: ${msg.heading.toFixed(0)}`;
  }

  // if it doesn't exist yet, create a new LineString object
  if (trajectory === undefined) {
    trajectory = new Feature({
      geometry: new LineString(fromLonLat([ msg.lon, msg.lat ])),
      meta_str : meta_str,
    });
    trajectory.setId(msg.mmsi);
    trajectory.setStyle(selectStyle(trajectory));
    live_targets[msg.mmsi] = trajectory;
    // lineSource.addFeature(trajectory);
  } else {
    // otherwise, update the linestring geometry with the new position
    // lineSource.removeFeature(trajectory);
    let coords = trajectory.getGeometry().getCoordinates();
    if (coords[-1, 0] === msg.lon && coords[-1, 1] === msg.lat) {
      return true;
    }
    coords.push(fromLonLat([ msg.lon, msg.lat ]));
    trajectory.getGeometry().setCoordinates(coords);

    // compress track using douglas-peucker algorithm
    if (trajectory.getGeometry().getCoordinates().length >= 100 &&
      trajectory.getGeometry().getCoordinates().length % 100 === 0) {
      trajectory.setGeometry(trajectory.getGeometry().simplify(100));
      // console.log(`decimated ${trajectory.get('mmsi')} ${trajectory.getGeometry().getCoordinates().length}`);
    }

    // enforce maximum number of points per vessel
    if (trajectory.getGeometry().getCoordinates().length > 150) {
      trajectory.getGeometry().setCoordinates(trajectory.getGeometry().getCoordinates().slice(-150));
    }
    /*
    trajectory.set('rot_latest', msg.rot);
    trajectory.set('sog_latest', msg.sog);
    trajectory.set('heading_latest', msg.heading);
    */
    trajectory.set('meta_str', meta_str);

    // map window will throw an error for linestring that is too short
    if (lineSource.getFeatureById(msg.mmsi) === null &&
      trajectory.getGeometry().getCoordinates().length === 2) {
      lineSource.addFeature(trajectory);
    }
  }
  // pointSource.addFeature(ft);
  return true;
};

