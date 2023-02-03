import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { defineConfig } from 'vite';

import { VitePWA } from 'vite-plugin-pwa';


const currentdir = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: resolve(currentdir),
  publicDir: resolve(currentdir, 'public'),
  build: {
    outDir: resolve(currentdir, '..', 'dist_map'),
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(currentdir, 'index.html'),
      }
    }
  },
  plugins: [
    VitePWA({
      selfDestroying: true,
      // registerType: 'autoUpdate',
      includeAssets: [ 'favicon.svg', 'robots.txt' ],
      manifest: {
        short_name: 'AISDB',
        name: 'AISDB: Ship Tracking Database',
        description: 'Web Interface for AIS Ship Tracking Database',
        icons: [
          {
            src: '/favicon.svg',
            sizes: 'any 192x192 256x256 512x512',
            type: 'image/svg+xml',
            purpose: 'monochrome any maskable'
          },
          {
            src: '/favicon.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'any maskable'
          }
        ],
        start_url: '/map/index.html',
        scope: '/map/',
        background_color : '#282c34',
        theme_color: '#282c34',
        display: 'fullscreen',
        orientation: 'landscape',
      }
    })
  ],

});
