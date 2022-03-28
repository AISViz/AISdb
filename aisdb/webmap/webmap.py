def place_marker(x, y, label=None):
    js = 'var placemarkLayer = new WorldWind.RenderableLayer("Placemark");\n'
    js += 'wwd.addLayer(placemarkLayer);\n'
    js += '''var placemarkAttributes = new WorldWind.PlacemarkAttributes(null);

placemarkAttributes.imageOffset = new WorldWind.Offset(
    WorldWind.OFFSET_FRACTION, 0.3,
    WorldWind.OFFSET_FRACTION, 0.0);

placemarkAttributes.labelAttributes.color = WorldWind.Color.YELLOW;
placemarkAttributes.labelAttributes.offset = new WorldWind.Offset(
            WorldWind.OFFSET_FRACTION, 0.5,
            WorldWind.OFFSET_FRACTION, 1.0);
    '''
    js += 'placemarkAttributes.imageSource = WorldWind.configuration.baseUrl + "images/pushpins/plain-red.png";\n'
    js += f'var position = new WorldWind.Position({y}, {x}, 100.0);\n'
    js += f'var placemark = new WorldWind.Placemark(position, false, placemarkAttributes);\n'
    print(js)
    return js


'''
import pyperclip

js = place_marker(-45.23, 61.07)
pyperclip.copy(js)

'''
import collada
from collada import *
import numpy as np
from tests.create_testing_data import sample_random_polygon

collada.geometry.Geometry.createPolygons()

geometry.Geometry

x, y = sample_random_polygon()

mesh = Collada()
effect = material.Effect("effect0", [],
                         "phong",
                         diffuse=(1, 0, 0),
                         specular=(0, 1, 0))
mat = material.Material("material0", "mymaterial", effect)
mesh.effects.append(effect)
mesh.materials.append(mat)

vrt_src
