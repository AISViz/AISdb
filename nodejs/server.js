const { networkInterfaces } = require('os');


const express = require('express');
const app = express();
const port = 8085;


app.use('/', express.static('../docs/html'));
//app.use('/rust', express.static('../docs/html/rust'));
//app.use('/rust/doc', express.static('../docs/html/rust/doc'));
app.use('/rust/doc/aisdb_bin', express.static('../docs/html/rust/doc/aisdb_bin'));
app.use('/rust/doc/aisdb_lib', express.static('../docs/html/rust/doc/aisdb_lib'));
app.use('/rust/doc/aisdb_bin/index.html', express.static('../docs/html/rust/doc/aisdb_bin/index.html'));
app.use('/rust/doc/aisdb_lib/index.html', express.static('../docs/html/rust/doc/aisdb_lib/index.html'));


app.listen(port, '::', () => {

  loop1: for (key of Object.keys(networkInterfaces()).reverse()) {

    if (['lo', '127.0.0.1', '::1'].indexOf(key) >= 0)
      continue loop1;

    loop2: for (net of networkInterfaces()[key].reverse()) {

      if (net['family'] == 'IPv4')
        addr = net['address'];

      else if (net['family'] == 'IPv6' && net['scopeid'] == 0)
        addr = `[${net['address']}]`;

      else
        continue loop2;

      console.log(`Docs available at http://${addr}:${port}\n`);

      break loop1;
    }
  }

})

