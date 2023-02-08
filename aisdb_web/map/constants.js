/** Socket server hostname as read from $VITE_AISDBHOST env variable
 * @constant {string} hostname
 */
let database_hostname = import.meta.env.VITE_AISDBHOST;
if (database_hostname === undefined || database_hostname === null) {
	database_hostname = 'localhost';
}

/** Socket server port as read from $VITE_AISDBPORT env variable
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
  export VITE_DISABLE_SSL=1
  npx vite ./aisdb_web/map/
  */
const disable_ssl = import.meta.env.VITE_DISABLE_SSL;
const use_bingmaps = import.meta.env.VITE_BINGMAPTILES;
const no_db_limit = import.meta.env.VITE_NO_DB_LIMIT;

export {
	database_hostname,
	database_port,
	disable_ssl,
	tileserver_hostname,
	use_bingmaps,
	no_db_limit,
};
