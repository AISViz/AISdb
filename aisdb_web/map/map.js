/**@module map */

import MousePosition from 'ol/control/MousePosition';
import View from 'ol/View';
import { ScaleLine } from 'ol/control.js';
import { default as DragBox } from 'ol/interaction/DragBox';
import { default as Draw } from 'ol/interaction/Draw';
import { default as Feature } from 'ol/Feature';
import { default as GeoJSON } from 'ol/format/GeoJSON';
import { default as Heatmap } from 'ol/layer/Heatmap';
import { default as Overlay } from 'ol/Overlay';
import { default as Point } from 'ol/geom/Point';
import { default as TileLayer } from 'ol/layer/Tile';
import { default as VectorLayer } from 'ol/layer/Vector';
import { default as VectorSource } from 'ol/source/Vector';
import { default as _Map } from 'ol/Map';
import { defaults as defaultInteractions } from 'ol/interaction';
import { fromLonLat } from 'ol/proj';
//import { createStringXY } from 'ol/coordinate';
//import { defaults as defaultControls } from 'ol/control';

import { vesselInfo } from './clientsocket.js';
import { debug, use_bingmaps } from './constants.js';
import { set_track_style } from './selectform.js';
import {
  dragBoxStyle,
  polySelectStyle,
  polyStyle,
  selectStyle,
  vesselStyles,
} from './palette.js';

/**Default map position */
const default_startpos = [ -63.5, 44.46 ];
const default_zoom = 10;


/**User search box */
window.searcharea = null;

/**Status message div item */
//const statusdiv = document.querySelector('#status-div');

/**Contains geometry for map selection feature */
const drawSource = new VectorSource({ wrapX: false });
/**Contains drawSource for map selection layer */
const drawLayer = new VectorLayer({ source: drawSource, zIndex: 5 });

/**Contains geometry for map zone polygons */
const polySource = new VectorSource({});
/**Contains polySource for map zone polygons */
const polyLayer = new VectorLayer({
  source: polySource,
  style: polyStyle, zIndex: 1,
});

/**Contains map vessel line geometries */
const lineSource = new VectorSource({});
/**Contains map lineSource layer */
const lineLayer = new VectorLayer({
  source: lineSource,
  style: vesselStyles.Unspecified,
  zIndex: 3,
});

const pointSource = new VectorSource({});
const pointLayer = new VectorLayer({
  source: pointSource,
  zIndex: 4,
});

/**Map heatmap source */
const heatSource = new VectorSource({});
/**Map heatmap layer */
const heatLayer = new Heatmap({
  source: heatSource,
  blur: 33,
  radius: 2.5,
  zIndex: 2,
});

/**Map window
 * @param {string} target target HTML item by ID
 * @param {Array} layers map layers to display
 * @param {ol/View) view default map view positioning
 */

/**Default map position
 * @see module:url
 */
const mapview = new View({ center: fromLonLat(default_startpos), zoom: default_zoom });

let map = null;

/*Map interactions */
let dragBox = null;
let draw = null;

/*Processing functions for incoming websocket data */
//let newHeatmapFeatures = null;
//let newPolygonFeature = null;
//let newTrackFeature = null;

/**Set a search area bounding box as determined by the extent
 * of currently selected polygons
 */
async function setSearchAreaFromSelected() {
  let alt_xmin = 180;
  let alt_xmax = -180;
  for (const ft of polySource.getFeatures()) {
    if (ft.get('selected') === true) {
      if (window.searcharea === null) {
        window.searcharea = { x0: 180, x1: -180, y1: 90, y0: -90 };
      }

      const coords = ft.getGeometry().clone()
        .transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
      for (const point of coords) {
        if (ft.get('meta_string').includes('_b') && point[0] < alt_xmin) {
          alt_xmin = point[0];
        } else if (!ft.get('meta_string').includes('_c') &&
          point[0] < window.searcharea.x0) {
          window.searcharea.x0 = point[0];
        }

        if (ft.get('meta_string').includes('_c') &&
          point[0] > alt_xmax) {
          alt_xmax = point[0];
        } else if (!ft.get('meta_string').includes('_b') &&
          point[0] > window.searcharea.x1) {
          window.searcharea.x1 = point[0];
        }

        if (point[1] < window.searcharea.y0) {
          window.searcharea.y0 = point[1];
        }

        if (point[1] > window.searcharea.y1) {
          window.searcharea.y1 = point[1];
        }
      }
    }
  }

  if (alt_xmin !== 180) {
    window.searcharea.x0 = alt_xmin;
  }

  if (alt_xmax !== -180) {
    window.searcharea.x1 = alt_xmax;
  }
}

/**New zone polygon feature
   * @param {Object} geojs GeoJSON Polygon object
   * @param {Object} meta metadata containing 'name'
   */
const newPolygonFeature = function (geojs, meta) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857',
  });
  feature.setId(meta.name);
  feature.set('meta', meta);
  feature.set('meta_string', meta.name); //this one controls the map label
  polySource.addFeature(feature);
};

/**Add vessel points to overall heatmap
   * @param {Array} xy Coordinate tuples
   */
const newHeatmapFeatures = function (xy) {
  xy.forEach(async (p) => {
    const pt = new Feature({
      geometry: new Point(fromLonLat(p)),
    });
    heatSource.addFeature(pt);
  });
};

/**New track geometry feature
   * @param {Object} geojs GeoJSON LineString object
   * @param {Object} id identifier
   */
const newTrackFeature = function (geojs, id) {
  const format = new GeoJSON();
  const feature = format.readFeature(geojs, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857',
  });
  feature.setId(id);
  set_track_style(feature);
  lineSource.addFeature(feature);
};

/**Callback for map pointermove event
   * @param {Vector} l ol VectorLayer
   * @returns {boolean}
   */
function pointermoveLayerFilterCallback(l) {
  if (l === lineLayer || l === polyLayer) {
    return true;
  }

  return false;
}

/**Callback for map click event
   * @param {Vector} l ol VectorLayer
   * @returns {boolean}
   */
function clickLayerFilterCallback(l) {
  if (l === polyLayer) {
    return true;
  }

  return false;
}

/**Initialize map layer and associated imports dynamically */
async function init_maplayers() {
  /**Ol map TileLayer */
  //the following env var will be set to "1" by the build script
  //if it detects the presence of env var $BINGMAPSKEY at build time
  let mapLayer = null;
  if (use_bingmaps !== undefined && use_bingmaps !== '' && use_bingmaps !== '0') {
    const { CustomBingMaps } = await import('./tileserver.js');
    mapLayer = new TileLayer({
      source: new CustomBingMaps({}),
      zIndex: 0,
    });
  } else {
    //Fall back to OSM if no API token was found by the build script
    //const { CustomOSM } = await import('./tileserver.js');
    //mapLayer = new TileLayer({ source: new CustomOSM({}) });

    const { default: OSM } = await import('ol/source/OSM');
    mapLayer = new TileLayer({
      source: new OSM({
        //url: 'https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png',
        url: 'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png'
      }),
    });
  }

  map = new _Map({
    target: 'mapDiv', //Div item in index.html
    layers: [ mapLayer, polyLayer, lineLayer, heatLayer, pointLayer, drawLayer ],
    view: mapview,
    interactions: defaultInteractions({ doubleClickZoom: false }),
    //Controls: defaultControls().extend([ mousePositionControl ]),
  });

  /**Coordinate display */
  const mousePositionControl = new MousePosition({
    //CoordinateFormat: createStringXY(4),
    coordinateFormat: function(coordinate) {
      const xy = [ coordinate[0].toFixed(4), coordinate[1].toFixed(4) ];
      return `${xy[0]}, ${xy[1]}`;
    },
    projection: 'EPSG:4326',
  });
  //mousePositionControl.on('mouseout', function() { });

  /**Scale bar display */
  const scaleControl = new ScaleLine({
    units: 'metric',
    bar: true,
    text: true,
    steps: 4,
    minWidth: 200,
    maxWidth: 256,
  });

  /**Add coordinate and scale bar displays to map controls */
  map.getControls().extend([ mousePositionControl ]);
  map.getControls().extend([ scaleControl ]);

  /*Cursor styling: indicate to the user that we are selecting an area */
  draw = new Draw({
    type: 'Point',
  });

  dragBox = new DragBox({});
  dragBox.on('boxend', () => {
    window.geom = dragBox.getGeometry();
    const selectFeature = new Feature({
      geometry: dragBox.getGeometry(),
      name: 'selectionArea',
    });
    selectFeature.setStyle(dragBoxStyle);
    drawSource.addFeature(selectFeature);
    map.removeInteraction(dragBox);
    document.body.style.cursor = 'initial';
  });

  /**Draw layer addfeature event */
  drawSource.on('addfeature', async () => {
    const selectbox = drawSource.getFeatures()[0].getGeometry().clone()
      .transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
    const x0 = Math.min(selectbox[0][0], selectbox[1][0],
      selectbox[2][0], selectbox[3][0], selectbox[4][0]);
    const x1 = Math.max(selectbox[0][0], selectbox[1][0],
      selectbox[2][0], selectbox[3][0], selectbox[4][0]);
    const y0 = Math.min(selectbox[0][1], selectbox[1][1],
      selectbox[2][1], selectbox[3][1], selectbox[4][1]);
    const y1 = Math.max(selectbox[0][1], selectbox[1][1],
      selectbox[2][1], selectbox[3][1], selectbox[4][1]);
    window.searcharea = { x0: x0, x1: x1, y0: y0, y1: y1 };
    map.removeInteraction(draw);
    map.removeInteraction(dragBox);
  });


  const overlay_container = document.querySelector('#overlay');
  const overlay_content = document.querySelector('#overlay-content');

  const overlay = new Overlay({
    element: overlay_container,
    //AutoPan: { animation: { duration: 250, }, },
  });

  map.addOverlay(overlay);

  let selected = null;
  let previous = null;
  map.on('pointermove', (e) => {
    if (selected !== null && selected.get('selected') !== true) {
      selected.setStyle(selectStyle);
      selected = null;
    } else if (selected !== null) {
      selected = null;
    }

    //reset track style to previous un-highlighted color
    if (previous !== null && selected !== null && previous.getId() !== selected.getId()) {
      previous.set('selected', false);
      if (previous.getGeometry().getType() === 'LineString') {
        set_track_style(previous);
      } else {
        previous.setStyle(polyStyle(previous));
      }
    }

    //highlight feature at cursor
    map.forEachFeatureAtPixel(e.pixel, (f) => {
      selected = f;
      const geomtype = f.getGeometry().getType();
      if (f.get('selected') !== true) {
        //Console.log(f.getProperties());
        if (geomtype === 'Polygon') {
          f.setStyle(polySelectStyle(f));
        } else if (geomtype === 'LineString') {
          f.setStyle(selectStyle(f));
        } else {
          console.log(`unexpected feature ${geomtype}`);
        }
      }
      //reset track style
      if (selected === null || previous !== null && previous.getStyle() === selectStyle) {
        previous.set('selected', false);
        if (previous.getGeometry().getType() === 'LineString') {
          set_track_style(previous);
        } else {
          previous.setStyle(polyStyle(previous));
        }
      }

      //Keep track of last feature so that styles can be reset after moving mouse
      if (previous === null) {
        previous = f;
      } else if (previous.getId() !== f.getId()) {
        previous = f;
      }

      return true;
    }, { layerFilter: pointermoveLayerFilterCallback },
    );

    //Show metadata for selected feature
    if (selected !== undefined && selected !== null) {
      overlay.setPosition(e.coordinate);
      const vinfo = vesselInfo[selected.getId()];
      if (vinfo !== undefined && 'meta_string' in vinfo) {
        overlay_content.innerHTML = vinfo.meta_string;
      } else if (!isNaN(selected.getId())){
        overlay_content.innerHTML = `MMSI: ${selected.getId()}<br>`;
      } else {
        overlay_content.innerHTML = `${selected.getId()}<br>`;
      }
    } else {
      overlay.setPosition(undefined);
      if (previous !== null) {
        previous.set('selected', false);
        if (previous.getGeometry().getType() === 'LineString') {
          set_track_style(previous);
        } else {
          previous.setStyle(polyStyle(previous));
        }
      }
    }
  });

  map.on('click', async (e) => {
    map.forEachFeatureAtPixel(e.pixel, async (f) => {
      if (f.get('selected') !== true) {
        f.setStyle(polySelectStyle(f));
        f.set('selected', true);
      } else {
        f.setStyle(polyStyle(f));
        f.set('selected', false);
      }

      window.searcharea = null;
      await setSearchAreaFromSelected();
      return true;
    }, { layerFilter: clickLayerFilterCallback },
    );
  });

  if (debug !== null && debug !== undefined) {
    console.log('done map initialization');
  }
}

export {
  dragBox,
  draw,
  drawSource,
  map,
  init_maplayers,
  lineSource,
  mapview,
  newHeatmapFeatures,
  newPolygonFeature,
  newTrackFeature,
  polySource,
  pointSource,
  setSearchAreaFromSelected,
};
