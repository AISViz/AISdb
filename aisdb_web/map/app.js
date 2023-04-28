import 'ol/ol.css';

window.addEventListener('load', async () => {
  const [
    { init_maplayers },
    { initialize_db_socket },
    { initialize_selectform },
    //{ initialize_stream_socket },
    { default: parseUrl },
    //{ },
  ] = await Promise.all([
    import('./map.js'),
    import('./clientsocket.js'),
    import('./selectform.js'),
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
    //import('./vessel_metadata.ts'),
    parseUrl(),
  ]);

  //await parseUrl();
  //console.log(process.env.TESTENV);

  const disable_stream = import.meta.env.VITE_DISABLE_STREAM;
  if (disable_stream !== null && disable_stream !== undefined) {
    let { initialize_stream_socket } = await import('./livestream.js');
    await initialize_stream_socket();
  }
});
