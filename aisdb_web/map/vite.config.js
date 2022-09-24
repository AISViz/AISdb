import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { defineConfig } from 'vite';

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
});
