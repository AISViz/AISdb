import 'ol/ol.css';
import { init_maplayers } from './map.js';

window.addEventListener('load', async () => {
  const map = await init_maplayers();

  const { initialize_db_socket } = await import('./clientsocket.js');
  window.socket = await initialize_db_socket();

  const [
    { createVesselMenuItem, mapHook, vesselmenu, vesseltypeselect },
    { vessellabels },
  ] = await Promise.all([
    import('./selectform.js'),
    import('./palette.js'),
  ]);

  await mapHook(map);

  createVesselMenuItem('All', 'All', '⋀');
  for (const label of vessellabels) {
    createVesselMenuItem(label, label);
  }

  createVesselMenuItem('Unknown', 'None', '○');
  vesseltypeselect.addEventListener('click', () => {
    vesselmenu.classList.toggle('show');
  });

  await import('./livestream.js');
});
