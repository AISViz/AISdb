(async () => {
  self.addEventListener('activate', (e) => {
    console.log('Unregistering service worker');
    self.registration.unregister()
      .then(() => {
        return self.clients.matchAll();
      })
      .then((clients) => {
        clients.forEach((client) => {
          console.log(`Navigating ${client.url}`);
          client.navigate(client.url);
        });
      });
  });

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

  await import('./livestream.js');
})();
