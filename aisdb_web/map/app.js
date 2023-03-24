import 'ol/ol.css';

window.addEventListener('load', async () => {
  const [
    { init_maplayers },
    { initialize_db_socket },
    { initialize_selectform },
    { initialize_stream_socket },
    { default: parseUrl },
    //{ },
  ] = await Promise.all([
    import('./map.js'),
    import('./clientsocket.js'),
    import('./selectform.js'),
    import('./livestream.js'),
    import('./url.js'),
    import('./vessel_metadata.ts'),
  ]);

  /*
  await initialize_selectform();
  await init_maplayers();
  await initialize_stream_socket();
  await initialize_db_socket();
  */
  await Promise.all([
    init_maplayers(),
    initialize_db_socket(),
    initialize_selectform(),
    initialize_stream_socket(),
    //import('./vessel_metadata.ts'),
  ]);


  await parseUrl();
});
