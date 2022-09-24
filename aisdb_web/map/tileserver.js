import { jsonp as requestJSONP } from 'ol/net';
import { get as getProjection } from 'ol/proj';
import BingMaps from 'ol/source/BingMaps';
import OSM from 'ol/source/OSM';

// import { hostname } from './clientsocket.js';
let hostname = import.meta.env.VITE_TILESERVER;

class CustomOSM extends OSM {
  /**
   * @param {Options} [options] Open Street Map options.
   */
  constructor(options) {
    options = options || {};

    let attributions = options.attributions;

    const crossOrigin =
      options.crossOrigin !== undefined ? options.crossOrigin : 'anonymous';

    let url = null;
    if (hostname.includes('127.0.0.1')) {
      url = options.url !== undefined ? options.url : '/{z}/{x}/{y}.png';
    } else {
      url = options.url !== undefined ? options.url : `https://${hostname}/{z}/{x}/{y}.png`;
    }

    super({
      // attributions: attributions,
      attributionsCollapsible: true,
      cacheSize: options.cacheSize,
      crossOrigin: crossOrigin,
      // interpolate: options.interpolate,
      interpolate: false,
      maxZoom: options.maxZoom !== undefined ? options.maxZoom : 19,
      opaque: options.opaque !== undefined ? options.opaque : true,
      reprojectionErrorThreshold: options.reprojectionErrorThreshold,
      tileLoadFunction: options.tileLoadFunction,
      transition: undefined,
      url: url,
      wrapX: true,
      zDirection: undefined,
    });
  }
}

class CustomBingMaps extends BingMaps{
  /**
   * @param {Options} options Bing Maps options.
   */
  constructor(options) {
    const hidpi = options.hidpi !== undefined ? options.hidpi : false;
    super({
      cacheSize: undefined,
      crossOrigin: 'anonymous',
      interpolate: false,
      opaque: true,
      projection: getProjection('EPSG:3857'),
      reprojectionErrorThreshold: 0.5,
      state: 'loading',
      tileLoadFunction: function (imageTile, src) {
        let [ _target, tiles, jpeg, req ] = src.replace('https://', '').split(/[/?]+/);
        if (hostname.includes('127.0.0.1')) {
          src = `/${tiles}/${jpeg}?${req}`;
        } else {
          src = `https://${hostname}/${tiles}/${jpeg}?${req}`;
        }
        imageTile.src_ = src;
        imageTile.getImage().src = src;
      },
      tilePixelRatio: hidpi ? 2 : 1,
      wrapX: true,
      transition: undefined,
      zDirection: undefined,
    });

    this.hidpi_ = hidpi;
    // this.culture_ = options.culture !== undefined ? options.culture : 'en-us';
    this.culture_ = 'en-us';
    this.maxZoom_ = -1;
    // this.imagerySet_ = 'Aerial';
    this.imagerySet_ = 'AerialWithLabels';


    // const url = `https://dev.virtualearth.net/REST/v1/Imagery/Metadata/${
    // this.imagerySet_}?uriScheme=https&include=ImageryProviders&key=${
    // this.apiKey_}&c=${this.culture_}`;

    const url = `https://${hostname}/REST/v1/Imagery/Metadata/${
      this.imagerySet_
    }?uriScheme=https&include=ImageryProviders&c=${
      this.culture_
    }`;
    requestJSONP(
      url,
      this.handleImageryMetadataResponse.bind(this),
      undefined,
      'jsonp'
    );
  }
}

export { CustomOSM, CustomBingMaps, };
