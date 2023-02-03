import OSM from 'ol/source/OSM';
import TileImage from 'ol/source/TileImage';
import { applyTransform, intersects } from 'ol/extent';
import { createFromTileUrlFunctions } from 'ol/tileurlfunction';
import { createOrUpdate } from 'ol/tilecoord';
import { createXYZ, extentFromProjection } from 'ol/tilegrid';
import { get as getProjection, getTransformFromProjections } from 'ol/proj';

import { tileserver_hostname } from './constants.js';


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
    // if (tileserver_hostname.includes('127.0.0.1')) {
    if (tileserver_hostname === '' || tileserver_hostname === '/') {
      url = options.url !== undefined ? options.url : '/{z}/{x}/{y}.png';
    } else {
      url = options.url !== undefined ? options.url : `https://${tileserver_hostname}/{z}/{x}/{y}.png`;
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


function quadKey(tileCoord) {
  const z = tileCoord[0];
  const digits = new Array(z);
  let mask = 1 << z - 1;
  let charCode = null;
  let i = null;
  for (i = 0; i < z; ++i) {
    // 48 is charCode for 0 - '0'.charCodeAt(0)
    charCode = 48;
    if (tileCoord[1] & mask) {
      charCode = charCode + 1;
    }
    if (tileCoord[2] & mask) {
      charCode = charCode + 2;
    }
    digits[i] = String.fromCharCode(charCode);
    mask = mask >> 1;
  }
  return digits.join('');
}


const TOS_ATTRIBUTION =
  '<a class="ol-attribution-bing-tos" ' +
  'href="https://www.microsoft.com/maps/product/terms.html" target="_blank">' +
  'Terms of Use</a>';


/**
  Create a custom bing maps class where the tileserver is overridden by
  the hostname set in $VITE_TILESERVER.
  The purpose is to enable tile caching in nginx to reduce number of API calls

  reference:
  https://github.com/openlayers/openlayers/blob/main/src/ol/source/BingMaps.js
  */
class CustomBingMaps extends TileImage {
  /**
   * @param {Options} options Bing Maps options.
   */
  constructor(options) {
    const hidpi = options.hidpi !== undefined ? options.hidpi : false;

    super({
      cacheSize: options.cacheSize,
      crossOrigin: 'anonymous',
      interpolate: options.interpolate,
      opaque: true,
      projection: getProjection('EPSG:3857'),
      reprojectionErrorThreshold: options.reprojectionErrorThreshold,
      state: 'loading',
      // tileLoadFunction: options.tileLoadFunction,
      tileLoadFunction: function (imageTile, src) {
        let [ _target, tiles, jpeg, req ] = src.replace('https://', '').split(/[/?]+/);
        // if (tileserver_hostname.includes('127.0.0.1')) {
        if (tileserver_hostname === '' || tileserver_hostname === '/') {
          src = `/${tiles}/${jpeg}?${req}`;
        } else {
          src = `https://${tileserver_hostname}/${tiles}/${jpeg}?${req}`;
        }
        imageTile.src_ = src;
        imageTile.getImage().src = src;
      },
      tilePixelRatio: hidpi ? 2 : 1,
      wrapX: options.wrapX !== undefined ? options.wrapX : true,
      transition: options.transition,
      zDirection: options.zDirection,
    });

    /**
     * @private
     * @type {boolean}
     */
    this.hidpi_ = hidpi;

    /**
     * @private
     * @type {string}
     */
    this.culture_ = options.culture !== undefined ? options.culture : 'en-us';

    /**
     * @private
     * @type {number}
     */
    this.maxZoom_ = options.maxZoom !== undefined ? options.maxZoom : -1;

    /**
     * @private
     * @type {string}
     */
    // this.apiKey_ = options.key;

    /**
     * @private
     * @type {string}
     */
    // this.imagerySet_ = options.imagerySet;
    this.imagerySet_ = 'Aerial';

    /*
    const url =
      'https://dev.virtualearth.net/REST/v1/Imagery/Metadata/' +
      this.imagerySet_ +
      '?uriScheme=https&include=ImageryProviders&key=' +
      this.apiKey_ +
      '&c=' +
      this.culture_;
      */

    let url = null;
    if (tileserver_hostname === '' || tileserver_hostname === '/') {
      url = '';
    } else {
      url = `https://${tileserver_hostname}`;
    }
    url = `${url }/REST/v1/Imagery/Metadata/${this.imagerySet_}?uriScheme=https&include=ImageryProviders&c=${this.culture_}`;

    fetch(url)
      .then((response) => {
        return response.json();
      })
      .then((json) => {
        return this.handleImageryMetadataResponse(json);
      });
  }

  /**
   * Get the api key used for this source.
   *
   * @returns {string} The api key.
   * @api
   */
  // getApiKey() {
  //  return this.apiKey_;
  // }

  /**
   * Get the imagery set associated with this source.
   *
   * @returns {string} The imagery set.
   * @api
   */
  getImagerySet() {
    return this.imagerySet_;
  }

  /**
   * @param {BingMapsImageryMetadataResponse} response Response.
   */
  handleImageryMetadataResponse(response) {
    if (
      response.statusCode !== 200 ||
      response.statusDescription !== 'OK' ||
      response.authenticationResultCode !== 'ValidCredentials' ||
      response.resourceSets.length !== 1 ||
      response.resourceSets[0].resources.length !== 1
    ) {
      this.setState('error');
      return;
    }

    const resource = response.resourceSets[0].resources[0];
    const maxZoom = this.maxZoom_ === -1 ? resource.zoomMax : this.maxZoom_;

    const sourceProjection = this.getProjection();
    const extent = extentFromProjection(sourceProjection);
    const scale = this.hidpi_ ? 2 : 1;
    const tileSize =
      resource.imageWidth === resource.imageHeight ?
        resource.imageWidth / scale :
        [ resource.imageWidth / scale, resource.imageHeight / scale ];

    const tileGrid = createXYZ({
      extent: extent,
      minZoom: resource.zoomMin,
      maxZoom: maxZoom,
      tileSize: tileSize,
    });
    this.tileGrid = tileGrid;

    const culture = this.culture_;
    const hidpi = this.hidpi_;
    this.tileUrlFunction = createFromTileUrlFunctions(
      resource.imageUrlSubdomains.map((subdomain) => {
        /** @type {import('../tilecoord.js').TileCoord} */
        const quadKeyTileCoord = [ 0, 0, 0 ];
        const imageUrl = resource.imageUrl
          .replace('{subdomain}', subdomain)
          .replace('{culture}', culture);
        return (
          /**
           * @param {import("../tilecoord.js").TileCoord} tileCoord Tile coordinate.
           * @param {number} pixelRatio Pixel ratio.
           * @param {import("../proj/Projection.js").default} projection Projection.
           * @return {string|undefined} Tile URL.
           */
          function (tileCoord, pixelRatio, projection) {
            if (!tileCoord) {
              return undefined;
            }
            createOrUpdate(
              tileCoord[0],
              tileCoord[1],
              tileCoord[2],
              quadKeyTileCoord
            );
            let url = imageUrl;
            if (hidpi) {
              url = `${url }&dpi=d1&device=mobile`;
            }
            return url.replace('{quadkey}', quadKey(quadKeyTileCoord));
          }
        );
      })
    );

    if (resource.imageryProviders) {
      const transform = getTransformFromProjections(
        getProjection('EPSG:4326'),
        this.getProjection()
      );

      this.setAttributions((frameState) => {
        const attributions = [];
        const viewState = frameState.viewState;
        const tileGrid = this.getTileGrid();
        const z = tileGrid.getZForResolution(
          viewState.resolution,
          this.zDirection
        );
        const tileCoord = tileGrid.getTileCoordForCoordAndZ(
          viewState.center,
          z
        );
        const zoom = tileCoord[0];
        resource.imageryProviders.map((imageryProvider) => {
          let intersecting = false;
          const coverageAreas = imageryProvider.coverageAreas;
          for (let i = 0, ii = coverageAreas.length; i < ii; ++i) {
            const coverageArea = coverageAreas[i];
            if (zoom >= coverageArea.zoomMin && zoom <= coverageArea.zoomMax) {
              const bbox = coverageArea.bbox;
              const epsg4326Extent = [ bbox[1], bbox[0], bbox[3], bbox[2] ];
              const extent = applyTransform(epsg4326Extent, transform);
              if (intersects(extent, frameState.extent)) {
                intersecting = true;
                break;
              }
            }
          }
          if (intersecting) {
            attributions.push(imageryProvider.attribution);
          }
        });

        attributions.push(TOS_ATTRIBUTION);
        return attributions;
      });
    }

    this.setState('ready');
  }
}


export { CustomOSM, CustomBingMaps, };
