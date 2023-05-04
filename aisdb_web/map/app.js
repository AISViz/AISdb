import 'ol/ol.css';

window.addEventListener('load', async () => {
  const [
    { init_maplayers },
    { initialize_db_socket },
    { initialize_selectform },
    //{ initialize_stream_socket },
    { default: parseUrl },
    { disable_stream },
    //{ },
  ] = await Promise.all([
    import('./map.js'),
    import('./clientsocket.js'),
    import('./selectform.js'),
    import('./url.js'),
    import('./vessel_metadata.ts'),
    import('./constants.js'),
  ]);

  await Promise.all([
    init_maplayers(),
    initialize_db_socket(),
    initialize_selectform(),
  ]);
  await parseUrl();

  if (disable_stream !== null && disable_stream !== undefined) {
    let { initialize_stream_socket } = await import('./livestream.js');
    await initialize_stream_socket();
  }
});
