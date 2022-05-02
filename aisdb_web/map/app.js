import './wasm_hook';
import './map';
import './selectform';
import './clientsocket';
import './render';

import { registerSW } from 'virtual:pwa-register';

window.addEventListener('load', () => {
  if ('serviceWorker' in navigator) {
    // && !/localhost/.test(window.location)) {
    /*
    const updateSW = registerSW({
      onNeedRefresh: function() {
        updateSW();
      },
    });
    */
    registerSW();
  }
});
