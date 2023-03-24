/**Socket server hostname as read from $VITE_AISDBHOST env variable
 * @constant {string} hostname
 */
let database_hostname = import.meta.env.VITE_AISDBHOST;
if (database_hostname === undefined || database_hostname === null) {
  database_hostname = 'localhost';
}

/**Socket server port as read from $VITE_AISDBPORT env variable
 * @constant {string} database_hostname
 */
let database_port = import.meta.env.VITE_AISDBPORT;
if (database_port === undefined) {
  database_port = '9924';
}

const tileserver_hostname = import.meta.env.VITE_TILESERVER;
if (tileserver_hostname === undefined) {
  console.log('tileserver hostname undefined');
}

/**
  For local testing, do:
  export VITE_DISABLE_SSL_STREAM=1
  export VITE_DISABLE_SSL_DB=1
  npx vite ./aisdb_web/map/
  */
const debug = import.meta.env.VITE_DEBUG;
const disable_ssl_db = import.meta.env.VITE_DISABLE_SSL_DB;
const disable_ssl_stream = import.meta.env.VITE_DISABLE_SSL_STREAM;
const no_db_limit = import.meta.env.VITE_NO_DB_LIMIT;
const use_bingmaps = import.meta.env.VITE_BINGMAPTILES;

export {
  database_hostname,
  database_port,
  debug,
  disable_ssl_db,
  disable_ssl_stream,
  tileserver_hostname,
  no_db_limit,
  use_bingmaps,
};
