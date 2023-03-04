import { Feature } from 'ol';
import { Fill, Stroke, Style, Circle } from 'ol/style';
import { LineString } from 'ol/geom';
import { fromLonLat } from 'ol/proj';

import { database_hostname, disable_ssl } from './constants.js';
import { lineSource } from './map.js';
import { livestreamStyle } from './palette.js';
// Import { Vector as VectorSource } from 'ol/source';
// Import Collection from 'ol/Collection';
// import WebGLPointsLayer from 'ol/layer/WebGLPoints';

/** on mousever feature style */

const ptSelectStyle = function (feature) {
  return new Style({
    image: new Circle({ radius: 4,
      fill: new Fill({
        color: 'rgba(255, 255, 255, 0.85)',
      }),
      stroke: new Stroke({
        // Color: 'rgba(10, 80, 255, 0.5)',
        color: 'rgba(100, 100, 100, 0.5)',
        width: 1,
      }) }),
  });
};

/* --- */

let streamsocket = null;
streamsocket = disable_ssl !== null && disable_ssl !== undefined ? new WebSocket(`ws://${database_hostname}:9922`) : new WebSocket(`wss://${database_hostname}:9922`);
const live_targets = {};

streamsocket.onerror = function (event) {
  // Const message = `livestream: an unexpected error occurred [${event.code}]`;
  streamsocket.close();
  streamsocket.onerror = null;
  streamsocket = null;
};

streamsocket.onmessage = function (event) {
  // Parse message
  const message = JSON.parse(event.data);

  // Create new point from message coordinates
  /*
  let ft = new Feature({
    geometry: new Point(fromLonLat([ msg.lon, msg.lat ])),
    mmsi: msg.mmsi,
  });
  ft.setStyle(ptSelectStyle);
  */

  // check target matrix for ship ID
  let trajectory = live_targets[message.mmsi];
  let meta_string = `mmsi: ${message.mmsi}`;
  if (message.sog >= 0) {
    meta_string = `${meta_string}&emsp;sog: ${message.sog.toFixed(1)}`;
  }

  if (message.rot >= 0) {
    meta_string = `${meta_string}&emsp;rot: ${message.rot.toFixed(1)}`;
  }

  if (message.heading >= 0) {
    meta_string = `${meta_string}&emsp;heading: ${message.heading.toFixed(0)}`;
  }

  // If it doesn't exist yet, create a new LineString object
  if (trajectory === undefined) {
    trajectory = new Feature({
      geometry: new LineString(fromLonLat([ message.lon, message.lat ])),
      meta_str: meta_string,
    });
    trajectory.setId(message.mmsi);
    trajectory.setStyle(livestreamStyle);
    trajectory.set('meta', { vesseltype_generic: '' });
    live_targets[message.mmsi] = trajectory;
  } else {
    // Otherwise, update the linestring geometry with the new position
    // lineSource.removeFeature(trajectory);
    const coords = trajectory.getGeometry().getCoordinates();
    if (coords[-1, 0] === message.lon && coords[-1, 1] === message.lat) {
      return true;
    }

    coords.push(fromLonLat([ message.lon, message.lat ]));
    trajectory.getGeometry().setCoordinates(coords);

    // Compress track using douglas-peucker algorithm
    if (trajectory.getGeometry().getCoordinates().length >= 100 &&
      trajectory.getGeometry().getCoordinates().length % 100 === 0) {
      trajectory.setGeometry(trajectory.getGeometry().simplify(100));
      // Console.log(`decimated ${trajectory.get('mmsi')} ${trajectory.getGeometry().getCoordinates().length}`);
    }

    // Enforce maximum number of points per vessel
    if (trajectory.getGeometry().getCoordinates().length > 150) {
      trajectory.getGeometry().setCoordinates(trajectory.getGeometry().getCoordinates().slice(-150));
    }

    /*
    Trajectory.set('rot_latest', msg.rot);
    trajectory.set('sog_latest', msg.sog);
    trajectory.set('heading_latest', msg.heading);
    */
    trajectory.set('meta_str', meta_string);

    // Map window will throw an error for linestring that is too short
    if (lineSource.getFeatureById(message.mmsi) === null &&
      trajectory.getGeometry().getCoordinates().length === 2) {
      lineSource.addFeature(trajectory);
    }
  }

  // PointSource.addFeature(ft);
  return true;
};
