import './map';
import './selectform';
import './clientsocket';
import './wasm_hook';
import parseUrl from './url';

import { registerSW } from 'virtual:pwa-register';

window.addEventListener('load', () => {
  parseUrl();

  if ('serviceWorker' in navigator) {
    // && !/localhost/.test(window.location)) {
    registerSW();
  }
});
