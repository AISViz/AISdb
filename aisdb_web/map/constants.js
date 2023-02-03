/** socket server hostname as read from $VITE_AISDBHOST env variable
 * @constant {string} hostname
 */
let database_hostname = import.meta.env.VITE_AISDBHOST;
if (database_hostname === undefined || database_hostname === null) {
  database_hostname = 'localhost';
}

/** socket server port as read from $VITE_AISDBPORT env variable
 * @constant {string} database_hostname
 */
let database_port = import.meta.env.VITE_AISDBPORT;
if (database_port === undefined) {
  database_port = '9924';
}

let tileserver_hostname = import.meta.env.VITE_TILESERVER;
if (tileserver_hostname === undefined) {
  console.log('tileserver hostname undefined');
}

/**
  for local testing, do:
  export VITE_DISABLE_SSL=1
  npx vite ./aisdb_web/map/
  */
let disable_ssl = import.meta.env.VITE_DISABLE_SSL;
let use_bingmaps = import.meta.env.VITE_BINGMAPTILES;


export {
  database_hostname,
  database_port,
  disable_ssl,
  tileserver_hostname,
  use_bingmaps
};
