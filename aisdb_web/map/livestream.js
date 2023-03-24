import { Feature } from 'ol';
import { Fill, Stroke, Style, Circle } from 'ol/style';
import { LineString } from 'ol/geom';
import { fromLonLat } from 'ol/proj';

import { database_hostname, disable_ssl_stream } from './constants.js';
import { lineSource } from './map.js';
import { livestreamStyle } from './palette.js';
import { set_track_style } from './selectform.js';
import { vesselInfo } from './clientsocket.js';
//Import { Vector as VectorSource } from 'ol/source';
//Import Collection from 'ol/Collection';
//import WebGLPointsLayer from 'ol/layer/WebGLPoints';

/**on mousever feature style */

const ptSelectStyle = function (feature) {
  return new Style({
    image: new Circle({ radius: 4,
      fill: new Fill({
        color: 'rgba(255, 255, 255, 0.85)',
      }),
      stroke: new Stroke({
        //Color: 'rgba(10, 80, 255, 0.5)',
        color: 'rgba(100, 100, 100, 0.5)',
        width: 1,
      }) }),
  });
};

/*--- */

async function initialize_stream_socket() {
  let streamsocket = null;
  if (disable_ssl_stream !== null && disable_ssl_stream !== undefined) {
    //Disable eslint
    streamsocket = new WebSocket(`ws://${database_hostname}:9922`);
    console.log('Caution: connecting to stream socket without TLS');
  } else {
    streamsocket = new WebSocket(`wss://${database_hostname}/stream`);
  }

  const live_targets = {};

  streamsocket.onerror = function (event) {
    //Const message = `livestream: an unexpected error occurred [${event.code}]`;
    streamsocket.close();
    streamsocket.onerror = null;
    streamsocket = null;
  };

  streamsocket.onmessage = function (event) {
    //Parse message
    const message = JSON.parse(event.data);

    //Check target matrix for ship ID
    let trajectory = live_targets[message.mmsi];

    //If it doesn't exist yet, create a new LineString object
    if (trajectory === undefined) {
      trajectory = new Feature({
        geometry: new LineString(fromLonLat([ message.lon, message.lat ])),
        //Meta_str: meta_string,
        //meta: { mmsi: message.mmsi },
      });
      trajectory.setId(message.mmsi);
      trajectory.setStyle(livestreamStyle);
      //Trajectory.set('meta', { vesseltype_generic: '' });
      live_targets[message.mmsi] = trajectory;
    } else {
      //Otherwise, update the linestring geometry with the new position
      //lineSource.removeFeature(trajectory);
      const coords = trajectory.getGeometry().getCoordinates();
      if (coords[-1, 0] === message.lon && coords[-1, 1] === message.lat) {
        return true;
      }

      coords.push(fromLonLat([ message.lon, message.lat ]));
      trajectory.getGeometry().setCoordinates(coords);

      //Compress track using douglas-peucker algorithm
      if (trajectory.getGeometry().getCoordinates().length >= 100 &&
      trajectory.getGeometry().getCoordinates().length % 100 === 0) {
        trajectory.setGeometry(trajectory.getGeometry().simplify(100));
        //Console.log(`decimated ${trajectory.get('mmsi')} ${trajectory.getGeometry().getCoordinates().length}`);
      }

      //Enforce maximum number of points per vessel
      /*
    if (trajectory.getGeometry().getCoordinates().length > 150) {
      trajectory.getGeometry().setCoordinates(trajectory.getGeometry().getCoordinates().slice(-150));
    }
    */

      /*
    Trajectory.set('rot_latest', msg.rot);
    trajectory.set('sog_latest', msg.sog);
    trajectory.set('heading_latest', msg.heading);
    */
      if (message.mmsi in vesselInfo) {
        vesselInfo[message.mmsi].sog_latest = message.sog;
        vesselInfo[message.mmsi].heading = message.heading;
      }

      trajectory.set('meta', { mmsi: message.mmsi });

      //Map window will throw an error for linestring that is too short
      if (lineSource.getFeatureById(message.mmsi) === null &&
      trajectory.getGeometry().getCoordinates().length === 2) {
        set_track_style(trajectory);
        lineSource.addFeature(trajectory);
      }
    }

    //PointSource.addFeature(ft);
    return true;
  };

  return streamsocket;
}

export { initialize_stream_socket };
