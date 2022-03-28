var wwd = new WorldWind.WorldWindow("canvasOne");

wwd.addLayer(new WorldWind.BMNGOneImageLayer());
wwd.addLayer(new WorldWind.BMNGLandsatLayer());

wwd.addLayer(new WorldWind.CompassLayer());
wwd.addLayer(new WorldWind.CoordinatesDisplayLayer(wwd));
wwd.addLayer(new WorldWind.ViewControlsLayer(wwd));

//
//
//

function place_marker(x, y, z, label) {
  var placemarkLayer = new WorldWind.RenderableLayer("Placemark");
  wwd.addLayer(placemarkLayer);
  var placemarkAttributes = new WorldWind.PlacemarkAttributes(null);
  
  placemarkAttributes.imageOffset = new WorldWind.Offset(
      WorldWind.OFFSET_FRACTION, 0.3,
      WorldWind.OFFSET_FRACTION, 0.0);

  placemarkAttributes.labelAttributes.color = WorldWind.Color.YELLOW;
  placemarkAttributes.labelAttributes.offset = new WorldWind.Offset(
              WorldWind.OFFSET_FRACTION, 0.5,
              WorldWind.OFFSET_FRACTION, 1.0);
  //placemarkAttributes.imageSource = WorldWind.configuration.baseUrl + "images/pushpins/plain-red.png";
  placemarkAttributes.imageSource = WorldWind.configuration.baseUrl + "images/crosshair.png";
  var position = new WorldWind.Position(y, x, z);
  var placemark = new WorldWind.Placemark(position, true, placemarkAttributes);
  //placemark.label = "Placemark\n" +
  // "Lat " + placemark.position.latitude.toPrecision(4).toString() + "\n" +
  // "Lon " + placemark.position.longitude.toPrecision(5).toString();
  placemark.label = label + "\n";
  placemark.alwaysOnTop = true;
  placemarkLayer.addRenderable(placemark);
}

function place_polygon_txt(filename, z, label) {

  var polygonLayer = new WorldWind.RenderableLayer();
  wwd.addLayer(polygonLayer);

  var polygonAttributes = new WorldWind.ShapeAttributes(null);
  polygonAttributes.interiorColor = new WorldWind.Color(.8, .8, 8, 0.4);
  polygonAttributes.outlineColor = WorldWind.Color.GREY;
  polygonAttributes.drawOutline = true;
  polygonAttributes.applyLighting = true;

  var boundaries = [];
  boundaries.push(new WorldWind.Position(20.0, -75.0, 500.0));
  boundaries.push(new WorldWind.Position(25.0, -85.0, 500.0));
  boundaries.push(new WorldWind.Position(20.0, -95.0, 500.0));

  var polygon = new WorldWind.Polygon(boundaries, polygonAttributes);
  polygon.extrude = true;
  polygonLayer.addRenderable(polygon);

}

async function polygon_collada(filename) {
  var modelLayer = new WorldWind.RenderableLayer();
  wwd.addLayer(modelLayer);

  var position = new WorldWind.Position(10.0, -125.0, 800000.0);
  //var config = {dirPath: WorldWind.configuration.baseUrl + 'examples/collada_models/duck/'};
  var config = {dirPath: "assets/"};
  var colladaLoader = new WorldWind.ColladaLoader(position, config);
  //colladaLoader.load("duck.dae", function (colladaModel) {
  colladaLoader.load("sphere.dae", function (colladaModel) {
    colladaModel.scale = 1000;
    modelLayer.addRenderable(colladaModel);
  });
}

/*

place_marker(-63.61, 44.67, 100, "halifax");


var poly = [
  [
]

*/


