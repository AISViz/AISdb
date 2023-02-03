import 'ol/ol.css';
import { init_maplayers } from './map';

window.addEventListener('load', async () => {
  let map = await init_maplayers();

  let [
    { createVesselMenuItem, mapHook, vesselmenu, vesseltypeselect },
    { vessellabels },
  ] = await Promise.all([
    import('./selectform'),
    import('./palette'),
  ]);

  await mapHook(map);

  createVesselMenuItem('All', 'All', '⋀');
  for (let label of vessellabels) {
    createVesselMenuItem(label, label);
  }
  createVesselMenuItem('Unknown', 'None', '○');
  vesseltypeselect.onclick = function() {
    vesselmenu.classList.toggle('show');
  };

  await import('./livestream.js');
});
