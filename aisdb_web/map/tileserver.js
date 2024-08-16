/**Web map tile server module

    Create a custom bing maps class where the tileserver is overridden by
    the hostname set in $VITE_TILESERVER.
    Enables tile caching in nginx to reduce number of API calls

    Refer to:
    https://github.com/openlayers/openlayers/blob/main/src/ol/source/BingMaps.js
    https://github.com/openlayers/openlayers/blob/main/src/ol/source/OSM.js
*/
//import OSM from 'ol/source/OSM';
import XYZ from 'ol/source/XYZ';
import TileImage from 'ol/source/TileImage';
import { applyTransform, intersects } from 'ol/extent';
import { createFromTileUrlFunctions } from 'ol/tileurlfunction';
import { createOrUpdate } from 'ol/tilecoord';
import { createXYZ, extentFromProjection } from 'ol/tilegrid';
import { get as getProjection, getTransformFromProjections } from 'ol/proj';

import { tileserver_hostname } from './constants.js';

//class CustomOSM extends XYZ {
/**
   * @param {Options} [options] Open Street Map options.
   */
/*
  constructor(options) {
    options = options || {};

    const attributions = options.attributions;

    const crossOrigin =
      options.crossOrigin !== undefined ? options.crossOrigin : 'anonymous';

    let url = null;
    //If (tileserver_hostname.includes('127.0.0.1')) {
    if (tileserver_hostname === '' || tileserver_hostname === '/') {
      url = options.url !== undefined ? options.url : '/{z}/{x}/{y}.png';
    } else {
      url = options.url !== undefined ? options.url : `https://${tileserver_hostname}/{z}/{x}/{y}.png`;
    }

    super({
      //Attributions: attributions,
      attributionsCollapsible: true,
      cacheSize: options.cacheSize,
      crossOrigin: crossOrigin,
      //Interpolate: options.interpolate,
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
*/

function quadKey(tileCoord) {
  const z = tileCoord[0];
  const digits = new Array(z);
  let mask = 1 << z - 1;
  let charCode = null;
  let i = null;
  for (i = 0; i < z; ++i) {
    //48 is charCode for 0 - '0'.charCodeAt(0)
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

class CustomBingMaps extends TileImage {
  /**
   * @param {Options} options Bing Maps options.
      Reference:
      https://github.com/openlayers/openlayers/blob/main/src/ol/source/BingMaps.js
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
      //TileLoadFunction: options.tileLoadFunction,
      tileLoadFunction: function(imageTile, src) {
        const [ _target, tiles, jpeg, request ] = src.replace('https://', '').split(/[/?]+/);
        //If (tileserver_hostname.includes('127.0.0.1')) {
        src = tileserver_hostname === '' || tileserver_hostname === '/' ? `/${tiles}/${jpeg}?${request}` : `https://${tileserver_hostname}/${tiles}/${jpeg}?${request}`;

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
    //this.apiKey_ = options.key;

    /**
     * @private
     * @type {string}
     */
    //this.imagerySet_ = options.imagerySet;
    this.imagerySet_ = 'Aerial';

    /*
    Const url =
      'https://dev.virtualearth.net/REST/v1/Imagery/Metadata/' +
      this.imagerySet_ +
      '?uriScheme=https&include=ImageryProviders&key=' +
      this.apiKey_ +
      '&c=' +
      this.culture_;
      */
    let url = null;
    if (tileserver_hostname !== '' && tileserver_hostname !== '/') {
      url = `https://${tileserver_hostname}/REST/v1/Imagery/Metadata/${this.imagerySet_}?uriScheme=https&include=ImageryProviders&c=${this.culture_}`;
    } else {
      console.log(`info: got tileserver hostname ${tileserver_hostname}. Defaulting...`);
      url = `https://dev.virtualearth.net/REST/v1/Imagery/Metadata/${
        this.imagerySet_
      }?uriScheme=https&include=ImageryProviders&key=${
        this.apiKey_
      }&c=${
        this.culture_}`;
    }

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
  //getApiKey() {
  //return this.apiKey_;
  //}

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
        /**@type {import('../tilecoord.js').TileCoord} */
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
              quadKeyTileCoord,
            );
            let url = imageUrl;
            if (hidpi) {
              url = `${url}&dpi=d1&device=mobile`;
            }

            return url.replace('{quadkey}', quadKey(quadKeyTileCoord));
          }
        );
      }),
    );

    if (resource.imageryProviders) {
      const transform = getTransformFromProjections(
        getProjection('EPSG:4326'),
        this.getProjection(),
      );

      this.setAttributions((frameState) => {
        const attributions = [];
        const viewState = frameState.viewState;
        const tileGrid = this.getTileGrid();
        const z = tileGrid.getZForResolution(
          viewState.resolution,
          this.zDirection,
        );
        const tileCoord = tileGrid.getTileCoordForCoordAndZ(
          viewState.center,
          z,
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

export { CustomBingMaps };
