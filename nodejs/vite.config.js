import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const { resolve } = require('path')
// https://vitejs.dev/config/
//export default defineConfig({
//module.exports = ({
//  plugins: [react()]
//})

module.exports = {
  plugins: [react()],
  root: 'map',
  //root: '.',
  //build: {
    //rollupOptions: {
    //  input: {
    //    //main: resolve(__dirname, 'index.html'),
    //    map: resolve(__dirname, 'map/index.html'),
    //  }
  //  }
  //}
}
