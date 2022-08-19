import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { defineConfig } from 'vite';
// import { VitePWA } from 'vite-plugin-pwa';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default defineConfig({
  root: resolve(__dirname),
  publicDir: resolve(__dirname, 'public'),
  build: {
    outDir: resolve(__dirname, '..', 'dist_map'),
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
      }
    }
  },
  /*
  plugins: [
    VitePWA({
      registerType: 'autoUpdate',
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
        shortcuts: [
          {
            name: 'Ecoregions Shortcut',
            short_name: 'Ecoregions',
            description: 'Display map with ecoregion polygons overlay',
            url: '/map/?ecoregions',
            icons: [
              {
                src: '/favicon.svg',
                purpose: 'monochrome any',
                sizes: 'any 192x192 256x256 512x512',
                type: 'image/svg+xml'
              }
            ]
          }
        ]
      }
    })
  ],
  */
});
