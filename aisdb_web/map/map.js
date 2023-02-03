/** @module map */

import MousePosition from 'ol/control/MousePosition';
import View from 'ol/View';
import { createStringXY } from 'ol/coordinate';
import { default as Heatmap } from 'ol/layer/Heatmap';
import { default as TileLayer } from 'ol/layer/Tile';
import { default as VectorLayer } from 'ol/layer/Vector';
import { default as VectorSource } from 'ol/source/Vector';
import { default as _Map } from 'ol/Map';
import { defaults as defaultControls } from 'ol/control';
import { defaults as defaultInteractions } from 'ol/interaction';
import { fromLonLat } from 'ol/proj';

import { polyStyle, vesselStyles } from './palette.js';
import { use_bingmaps } from './constants.js';


window.searcharea = null;

/** status message div item */
const statusdiv = document.getElementById('status-div');

/** contains geometry for map selection feature */
let drawSource = new VectorSource({ wrapX: false });
/** contains drawSource for map selection layer */
let drawLayer = new VectorLayer({ source: drawSource, zIndex: 5 });

/** contains geometry for map zone polygons */
let polySource = new VectorSource({});
/** contains polySource for map zone polygons */
let polyLayer = new VectorLayer({
  source: polySource,
  style: polyStyle, zIndex: 1,
});

/** contains map vessel line geometries */
let lineSource = new VectorSource({});
/** contains map lineSource layer */
let lineLayer = new VectorLayer({
  source: lineSource,
  style: vesselStyles.Unspecified,
  zIndex: 3,
});

let pointSource = new VectorSource({});
let pointLayer = new VectorLayer({
  source: pointSource,
  zIndex: 4,
});

/** map heatmap source */
let heatSource = new VectorSource({ });
/** map heatmap layer */
let heatLayer = new Heatmap({
  source: heatSource,
  blur: 33,
  radius: 2.5,
  zIndex: 2,
});

/** map window
 * @param {string} target target HTML item by ID
 * @param {Array} layers map layers to display
 * @param {ol/View) view default map view positioning
 */

/* map objects */
let dragBox = null;
let draw = null;
// let map = null;
// let mapview = null;

/** default map position
 * @see module:url
 */
let mapview = new View({
  center: fromLonLat([ -63.5, 44.6 ]), // east
  // center: proj.fromLonLat([-123.0, 49.2]), //west
  // center: proj.fromLonLat([ -100, 57 ]), // canada
  zoom: 11,
});
const mousePositionControl = new MousePosition({
  coordinateFormat: createStringXY(4),
  projection: 'EPSG:4326',
});


/* processing functions for incoming websocket data */
let newHeatmapFeatures = null;
let newPolygonFeature = null;
let newTrackFeature = null;


/** set a search area bounding box as determined by the extent
 * of currently selected polygons
 */
async function setSearchAreaFromSelected() {
  let alt_xmin = 180;
  let alt_xmax = -180;
  for (let ft of polySource.getFeatures()) {
    if (ft.get('selected') === true) {
      if (window.searcharea === null) {
        window.searcharea = { minX: 180, maxX:-180, minY:90, maxY:-90 };
      }
      let coords = ft.getGeometry().clone()
        .transform('EPSG:3857', 'EPSG:4326').getCoordinates()[0];
      for (let point of coords) {
        if (ft.get('meta_str').includes('_b') && point[0] < alt_xmin) {
          alt_xmin = point[0];
        } else if (!ft.get('meta_str').includes('_c') &&
          point[0] < window.searcharea.minX) {
          window.searcharea.minX = point[0];
        }

        if (ft.get('meta_str').includes('_c') &&
          point[0] > alt_xmax) {
          alt_xmax = point[0];
        } else if (!ft.get('meta_str').includes('_b') &&
          point[0] > window.searcharea.maxX) {
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
  if (alt_xmin !== 180) {
    window.searcharea.minX = alt_xmin;
  }
  if (alt_xmax !== -180) {
    window.searcharea.maxX = alt_xmax;
  }
}

/** initialize map layer and associated imports dynamically */
async function init_maplayers() {
  let [
    { set_track_style },
    { default: Feature },
    { default: GeoJSON },
    { default: Point },
    { default: DragBox },
    { default: Draw },
  ] = await Promise.all([
    import('./selectform.js'),
    import('ol/Feature'),
    import('ol/format/GeoJSON'),
    import('ol/geom/Point'),
    import('ol/interaction/DragBox'),
    import('ol/interaction/Draw'),
  ]);

  let {
    dragBoxStyle,
    polySelectStyle,
    selectStyle,
    vesseltypes,
  } = await import('./palette');

  /** ol map TileLayer */
  // the following env var will be set to "1" by the build script
  // if it detects the presence of env var $BINGMAPSKEY at build time
  let mapLayer = null;
  if (use_bingmaps !== undefined && use_bingmaps !== '' && use_bingmaps !== '0') {
    let { CustomBingMaps } = await import('./tileserver.js');
    mapLayer = new TileLayer({
      source: new CustomBingMaps({}),
      zIndex: 0,
    });
  } else {
    // fall back to OSM if no API token was found by the build script
    let { CustomOSM } = await import('./tileserver.js');
    mapLayer = new TileLayer({ source: new CustomOSM({}) });
  }

  let map = new _Map({
    target: 'mapDiv', // div item in index.html
    // layers: [ mapLayer, polyLayer, lineLayer, heatLayer, pointLayer, drawLayer ],
    view: mapview,
    interactions: defaultInteractions({ doubleClickZoom:false }),
    controls: defaultControls().extend([ mousePositionControl ]),
  });


  /* cursor styling: indicate to the user that we are selecting an area */
  draw = new Draw({
    type: 'Point',
  });

  dragBox = new DragBox({});
  dragBox.on('boxend', () => {
    window.geom = dragBox.getGeometry();
    let selectFeature = new Feature({
      geometry: dragBox.getGeometry(),
      name: 'selectionArea',
    });
    selectFeature.setStyle(dragBoxStyle);
    drawSource.addFeature(selectFeature);
    map.removeInteraction(dragBox);
    document.body.style.cursor = 'initial';
  });

  /** draw layer addfeature event */
  drawSource.on('addfeature', async () => {
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

  /** callback for map pointermove event
   * @param {Vector} l ol VectorLayer
   * @returns {boolean}
   */
  function pointermoveLayerFilterCallback(l) {
    if (l === lineLayer || l === polyLayer) {
      return true;
    }
    return false;
  }

  /** callback for map click event
   * @param {Vector} l ol VectorLayer
   * @returns {boolean}
   */
  function clickLayerFilterCallback(l) {
    if (l === polyLayer) {
      return true;
    }
    return false;
  }

  /** new zone polygon feature
   * @param {Object} geojs GeoJSON Polygon object
   * @param {Object} meta geometry metadata
   */
  newPolygonFeature = async function(geojs, meta) {
    const format = new GeoJSON();
    const feature = format.readFeature(geojs, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857',
    });
    feature.setProperties({ meta_str: meta.name });
    polySource.addFeature(feature);
  };

  /** add vessel points to overall heatmap
   * @param {Array} xy Coordinate tuples
   */
  newHeatmapFeatures = async function(xy) {
    xy.forEach(async (p) => {
      let pt = new Feature({
        geometry: new Point(fromLonLat(p)),
      });
      heatSource.addFeature(pt);
    });
  };

  /** new track geometry feature
   * @param {Object} geojs GeoJSON LineString object
   * @param {Object} meta geometry metadata
   */
  newTrackFeature = async function(geojs, meta) {
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
  };


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
      let geomtype = f.getGeometry().getType();
      if (f.get('selected') !== true) {
        // console.log(f.getProperties());
        if (geomtype === 'Polygon') {
          f.setStyle(polySelectStyle(f));
        } else if (geomtype === 'LineString') {
          f.setStyle(selectStyle(f));
        } else {
          console.log(`unexpected feature ${geomtype}`);
        }
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
    }, { layerFilter: clickLayerFilterCallback }
    );
  });


  // layers: [ mapLayer, polyLayer, lineLayer, heatLayer, pointLayer, drawLayer ],
  map.setLayers([ mapLayer, polyLayer, lineLayer, heatLayer, pointLayer, drawLayer ]);

  return map;
}

export {
  dragBox,
  draw,
  drawSource,
  init_maplayers,
  lineSource,
  // map,
  mapview,
  newHeatmapFeatures,
  newPolygonFeature,
  newTrackFeature,
  polySource,
  pointSource,
  setSearchAreaFromSelected,
};
