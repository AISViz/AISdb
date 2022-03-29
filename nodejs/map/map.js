//import BingMaps from 'ol/source/BingMaps';

var map = new ol.Map({
	target: 'map', //div item in index.html
	layers: [
		new ol.layer.Tile({
			source: new ol.source.OSM()
		})
	],
	view: new ol.View({
		center: ol.proj.fromLonLat([-63.6, 44.6]),
		zoom: 5
	})
});
