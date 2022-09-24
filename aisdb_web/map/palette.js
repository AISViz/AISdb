/**
 * map styling and colorschemes
 * @module palette
 */
import { Fill, Stroke, Style, Text } from 'ol/style';


/*
 * polygon text styles
 */

let polygonText = function(feature) {
  return new Text({
    stroke: new Stroke({ color: 'white', width: 3 }),
    align: 'center',
    text: feature.get('meta_str'),
    // font: '3 12 / 12 12', // weight size / height dom.font.value
    overflow: true,
    fill: new Fill({ color: 'black' }),
  });
};

/** default zone polygon map style */

const polyStyle = function(feature) {
  return new Style({
    stroke: new Stroke({
      color: '#000000',
    }),
    fill: new Fill({
      color: 'rgba(255,255,255,0.3)',
    }),
    text: polygonText(feature),
  });
};
const polySelectStyle = function(feature) {
  return new Style({
    fill: new Fill({
      // color: '#eeeeee',
      color: 'rgba(255, 255, 255, 0.4)',
    }),
    stroke: new Stroke({
      color: 'rgba(255, 255, 255, 0.7)',
      width: 4,
    }),
    text: polygonText(feature),
  });
};

/** on mousever feature style */
const selectStyle = function(feature) {
  return new Style({
    fill: new Fill({
      // color: '#eeeeee',
      color: 'rgba(255, 255, 255, 0.4)',
    }),
    stroke: new Stroke({
      color: 'rgba(255, 255, 255, 0.7)',
      width: 4,
    }),
  });
};


/** hidden feature style */
const hiddenStyle = new Style({
  fill: new Fill({
    color: 'rgba(255, 255, 255, 0)',
  }),
  stroke: new Stroke({
    color: 'rgba(255, 255, 255, 0)',
    width: 0,
  }),
});

/** map window selection area feature style */
const dragBoxStyle = new Style({
  fill: new Fill({
    color: 'rgba(255, 255, 255, 0)',
  }),
  stroke: new Stroke({
    color: 'rgba(0, 200, 255, .5)',
    width: 3,
  }),
});

/** tracks color palette */
const palette = [
  // [0, 0, 0],
  // [1, 0, 103],
  [ 213, 255, 0 ],
  [ 255, 0, 86 ],
  [ 0, 155, 255 ],
  [ 158, 0, 142 ],
  // [14, 76, 161],
  // [0, 95, 57],
  [ 255, 110, 65 ],
  [ 0, 255, 0 ],
  // [149, 0, 58],
  // [255, 147, 126],
  // [164, 36, 0],
  // [0, 21, 68],
  // [145, 208, 203],
  // [98, 14, 0],
  // [107, 104, 130],
  // [0, 0, 255],
  // [0, 125, 181],
  // [106, 130, 108],
  [ 0, 174, 126 ],
  [ 194, 140, 159 ],
  [ 190, 153, 112 ],
  [ 0, 143, 156 ],
  [ 95, 173, 78 ],
  [ 255, 0, 0 ],
  [ 255, 0, 246 ],
  [ 255, 2, 157 ],
  [ 104, 61, 59 ],
  [ 255, 116, 163 ],
  [ 152, 255, 82 ],
  [ 150, 138, 232 ],
  [ 167, 87, 64 ],
  [ 1, 255, 254 ],
  // [255, 238, 232],
  [ 254, 137, 0 ],
  [ 255, 229, 2 ],
  [ 1, 208, 255 ],
  [ 187, 136, 0 ],
  // [117, 68, 177],
  // [165, 255, 210],
  [ 255, 166, 254 ],
  [ 119, 77, 0 ],
  // [122, 71, 130],
  // [38, 52, 0],
  // [0, 71, 84],
  // [67, 0, 44],
  [ 181, 0, 255 ],
  [ 255, 177, 103 ],
  // [255, 219, 102],
  [ 144, 251, 146 ],
  [ 126, 45, 210 ],
  [ 189, 211, 147 ],
  [ 229, 111, 254 ],
  // [222, 255, 116],
  [ 0, 255, 120 ],
  [ 189, 198, 255 ],
  // [0, 100, 1],
  [ 0, 118, 255 ],
  [ 133, 169, 0 ],
  // [0, 185, 23],
  [ 120, 130, 49 ],
  [ 0, 255, 198 ],
  [ 232, 94, 190 ],
];

/** known vessel types */
const vessellabels = [
  // '',
  // '-',
  // 'Unspecified',
  'Anti-Pollution',
  'Beacon, Starboard Hand',
  'Cargo',
  'Dive Vessel',
  // 'Cargo - Hazard A (Major)',
  // 'Cargo - Hazard B',
  // 'Cargo - Hazard C (Minor)',
  // 'Cargo - Hazard D (Recognizable)',
  'Dredger',
  'Fishing',
  'High Speed Craft',
  'Isolated Danger',
  'Law Enforce',
  'Local Vessel',
  'Manned VTS',
  'Military Ops',
  'Medical Trans',
  'Other',
  'Passenger',
  'Pilot Vessel',
  'Pleasure Craft',
  'Port Hand Mark',
  'Port Tender',
  'Reference Point',
  'Reserved',
  'SAR',
  'SAR Aircraft',
  'Sailing Vessel',
  'Special Craft',
  'Safe Water',
  'Tanker',
  // 'Tanker - Hazard A (Major)',
  // 'Tanker - Hazard B',
  // 'Tanker - Hazard C (Minor)',
  // 'Tanker - Hazard D (Recognizable)',
  'Tug',
  'Wing In Grnd',
];

let vesseltypes = {};
vessellabels.forEach((key, i) => {
  vesseltypes[key] = palette[i];
});
vessellabels.forEach((key, i) => {
  vesseltypes[key.replace(/\s/g, '')] = palette[i];
});
vesseltypes[''] = '#EEEEEE';
vesseltypes['-'] = '#EEEEEE';
vesseltypes.Unspecified = '#EEEEEE';
vesseltypes['Cargo - Hazard A (Major)'] = vesseltypes.Cargo;
vesseltypes['Cargo - Hazard B'] = vesseltypes.Cargo;
vesseltypes['Cargo - Hazard C (Minor)'] = vesseltypes.Cargo;
vesseltypes['Cargo - Hazard D (Recognizable)'] = vesseltypes.Cargo;
vesseltypes['Cargo-HazardA(Major)'] = vesseltypes.Cargo;
vesseltypes['Cargo-HazardB'] = vesseltypes.Cargo;
vesseltypes['Cargo-HazardC(Minor)'] = vesseltypes.Cargo;
vesseltypes['Cargo-HazardD(Recognizable)'] = vesseltypes.Cargo;
vesseltypes['Tanker - Hazard A (Major)'] = vesseltypes.Tanker;
vesseltypes['Tanker - Hazard B'] = vesseltypes.Tanker;
vesseltypes['Tanker - Hazard C (Minor)'] = vesseltypes.Tanker;
vesseltypes['Tanker - Hazard D (Recognizable)'] = vesseltypes.Tanker;
vesseltypes['Tanker-HazardA(Major)'] = vesseltypes.Tanker;
vesseltypes['Tanker-HazardB'] = vesseltypes.Tanker;
vesseltypes['Tanker-HazardC(Minor)'] = vesseltypes.Tanker;
vesseltypes['Tanker-HazardD(Recognizable)'] = vesseltypes.Tanker;

/** maps vessellabels to colors in palette */
let vesselStyles = {};
// vesseltypes.forEach((key) => {
for (const key of Object.keys(vesseltypes)) {
  vesselStyles[key] = new Style({
    stroke: new Stroke({
      color: vesseltypes[key],
      width: 1,
    }),
    fill: new Fill({
      color: vesseltypes[key],
    }),
  });
}

export {
  dragBoxStyle,
  hiddenStyle,
  polyStyle,
  polySelectStyle,
  selectStyle,
  vesselStyles,
  vessellabels,
  vesseltypes,
};
