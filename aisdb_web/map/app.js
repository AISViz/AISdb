(async () => {
  let { init_maplayers } = await import('./map');
  let [
    { createVesselMenuItem, vesselmenu, vesseltypeselect },
    { vessellabels },
  ] = await Promise.all([
    import('./selectform'),
    import('./palette'),
    init_maplayers(),
  ]);

  createVesselMenuItem('All', 'All', '⋀');
  for (let label of vessellabels) {
    createVesselMenuItem(label, label);
  }
  createVesselMenuItem('Unknown', 'None', '○');
  vesseltypeselect.onclick = function() {
    vesselmenu.classList.toggle('show');
  };

  // await import('./public/tileserver');
  await import('./livestream.js');
})();
