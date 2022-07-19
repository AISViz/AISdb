import './map';

window.addEventListener('load', () => {
  if ('serviceWorker' in navigator) {
    import('virtual:pwa-register').then(({ registerSW }) => {
      registerSW();
    });
    // && !/localhost/.test(window.location)) {
    /*
    const updateSW = registerSW({
      onNeedRefresh: function() {
        updateSW();
      },
    });
    */
    // registerSW();
  }
});

