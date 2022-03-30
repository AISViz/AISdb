import 'ol/ol.css';
import BingMaps from 'ol/source/BingMaps';
import Map from 'ol/Map';
import TileLayer from 'ol/layer/Tile';
import View from 'ol/View';

const styles = [
  'Aerial',
  'AerialWithLabelsOnDemand',
  'CanvasDark',
  'RoadOnDemand',
  //'OrdnanceSurvey',
];



const layers = [];
let i, ii;
for (i = 0, ii = styles.length; i < ii; ++i) {
  layers.push(
    new TileLayer({
      visible: false,
      preload: Infinity,
      source: new BingMaps({
        //key: 'Your Bing Maps Key from https://www.bingmapsportal.com/ here',
        key: 'Atkf6bjY5ujuqD30tybOIc74JapBW85_kHMeaB3mk4euiw-Fq-EYKRijEPSfhUDM',
        imagerySet: styles[i],
        // use maxZoom 19 to see stretched tiles instead of the BingMaps
        // "no photos at this zoom level" tiles
        // maxZoom: 19
      }),
    })
  );
}


var map = new ol.Map({
  target: 'map', //div item in index.html
  /*
  layers: [
    new ol.layer.Tile({
      source: new ol.source.OSM()
    })
  ],
  */
  layers: layers,
  view: new ol.View({
    //center: ol.proj.fromLonLat([-63.6, 44.6]),
    center: ol.proj.fromLonLat([-63.6, 44.6]),
    zoom: 8
  })
});



const select = document.getElementById('layer-select');
function onChange() {
  const style = select.value;
  for (let i = 0, ii = layers.length; i < ii; ++i) {
    layers[i].setVisible(styles[i] === style);
  }
}
select.addEventListener('change', onChange);
onChange();
